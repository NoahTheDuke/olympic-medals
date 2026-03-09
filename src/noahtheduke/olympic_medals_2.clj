(ns noahtheduke.olympic-medals-2
  (:require
   [babashka.fs :as fs]
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.walk :refer [postwalk]]
   [datalevin.core :as d]
   [datalevin.validate :as dv]
   [hickory.core :refer [as-hiccup parse]]
   [noahtheduke.olympic-medals.data :refer [country-code->name country-names season]]
   [noahtheduke.splint.pattern :refer [pattern]]))
#'country-names

(set! *warn-on-reflection* true)

(defn make-schema [s]
  (dv/validate-schema s)
  s)

(def schema
  (make-schema
    {
     :game/url {:db/valueType :db.type/string
                :db/unique :db.unique/identity}
     :game/year {:db/valueType :db.type/string}
     :game/city {:db/valueType :db.type/string}

     :sport/url {:db/valueType :db.type/string
                 :db/unique :db.unique/identity}
     :sport/name {:db/valueType :db.type/string}
     :sport/game {:db/valueType :db.type/ref}

     :event/url {:db/valueType :db.type/string
                 :db/unique :db.unique/identity}
     :event/name {:db/valueType :db.type/string}
     :event/game {:db/valueType :db.type/ref}
     :event/sport {:db/valueType :db.type/ref}

     :result/status {:db/valueType :db.type/string}
     :result/year {:db/valueType :db.type/long}
     :result/month {:db/valueType :db.type/long}
     :result/day {:db/valueType :db.type/long}
     :result/winner {:db/valueType :db.type/string}
     :result/country {:db/valueType :db.type/string}
     :result/medal {:db/valueType :db.type/string}
     :result/game {:db/valueType :db.type/ref}
     :result/sport {:db/valueType :db.type/ref}
     :result/event {:db/valueType :db.type/ref}
     }))

(def conn (d/get-conn "./db/om.db" schema))

(defn edn-read [file]
  (edn/read-string {:default tagged-literal} (slurp (str file))))

(defn link-dispatch [pending-url] (:type pending-url))

(def base-url "https://www.olympedia.org")

(defn parse-page
  [fragment]
  (let [html (-> (client/get (str base-url fragment))
               :body
               (parse)
               (as-hiccup))
        #_#_path (io/file "data" "olympedia" (str (subs fragment 1) ".edn"))]
    ; (io/make-parents path)
    ; (spit path (pr-str html))
    html))

(defmulti get-links {:arglists '([{:as pending-url :keys [type url extra html]}])} #'link-dispatch)
(remove-all-methods get-links)
(defmethod get-links :default [pending-url] (throw (ex-info "default" pending-url)))

(def game-pat
  (pattern
   '[:tr (?* _)
     [:td (? _ map?) (?* _)]
     "\n"
     [:td (? _ map?) [:a {:href (? ?url string?)} (? ?year string?)]]
     "\n"
     [:td (? _ map?) [:a {:href (? ?url string?)} (? ?city string?)]]
     (?* _)]))

(defmethod get-links :game [pending-url]
  (prn :game (:url pending-url))
  (let [html (or (:html pending-url) (parse-page (:url pending-url)))]
    (postwalk
     (fn [obj]
       (when-let [{:syms [?year ?city ?url]} (game-pat obj)]
         (when (not= "Olympia" ?city)
           (d/transact! conn
             [{:game/year ?year
               :game/city ?city
               :game/url ?url}])))
       obj)
     html)
    nil))

(comment
  (get-links {:type :game :url "/editions"})
  (let [id (-> (d/q [:find '?e
                     :where ['?e :game/url "/editions/3"]]
                 (d/db conn))
             (ffirst)) ]
    id))

(defn get-url
  [url]
  (and (string? url)
       (re-find #"/editions/\d+/sports" url)))

(def sport-pat
  (pattern
   '[:td
     (?* _)
     [:a {:href (? ?url get-url)} (? ?name string?)]
     (?* _)]))

(comment
  (d/q '[:find ?g
             :in $ ?game-url ?event-url
             :where
             [?g :game/url ?game-url]
             #_(not-join [?g]
               [?s :sport/game ?g]
               [?s :sport/url ?event-url])]
        (d/db conn)
        "/editions/1"
        "poop"))

(defmethod get-links :sport [pending-url]
  (prn :sport (:url pending-url))
  (let [html (or (:html pending-url) (parse-page (:url pending-url)))]
    (postwalk
     (fn [obj]
       (when-let [{:syms [?url ?name]} (sport-pat obj)]
         (let [[_ game-url] (re-find #"(/editions/\d+).*" ?url)
               game (-> (d/q '[:find ?g
                               :in $ ?game-url
                               :where
                               [?g :game/url ?game-url]]
                          (d/db conn) game-url)
                      (ffirst))]
           (when game
             (d/transact! conn
               [{:sport/url ?url
                 :sport/name ?name
                 :sport/game game}]))))
       obj)
     html)
    nil))

(comment
  (get-links {:type :sport :url "/editions/1"}))

(defn event-url
  [?url]
  (and (string? ?url)
       (re-find #"/results/\d+" ?url)))

(def event-pat
  (pattern
   '[:td (? _ map?) [:a {:href (? ?url event-url)} (? ?name string?)]]))

(defmethod get-links :event [pending-url]
  (prn :event (:url pending-url))
  (let [html (:html pending-url)]
    (postwalk
     (fn [obj]
       (when-let [{:syms [?url ?name]} (event-pat obj)]
         (let [[_ sport-url game-url] (re-find #"((/editions/\d+)/sports/.*)\.edn" (:file pending-url))
               [game sport]
               (first (d/q '[:find ?g ?s
                             :in $ ?game-url ?sport-url
                             :where
                             [?g :game/url ?game-url]
                             [?s :sport/game ?g]
                             [?s :sport/url ?sport-url]]
                        (d/db conn) game-url sport-url))]
           (when game
             (d/transact! conn
               [{:event/url ?url
                 :event/name ?name
                 :event/game game
                 :event/sport sport}]))))
       obj)
     html)
    nil))

(comment
  (get-links {:type :event
              :sport/url "/editions/1/sports/WRE"})
  (re-find #"((/editions/\d+)/sports/.*)\.edn" "/editions/1/sports/WRE.edn"))

(def status-pat
  (pattern
   '[:tr {} [:th {} "Status"] [:td {} ?status]]))

(def date-pat
  (pattern
   '[:table {:class "biodata"} (?* _)
     [:tbody (? _ map?) (?* _)
      [:tr (? _ map?)
       (?* _)
       [:th (? _ map?) "Date"]
       (?* _)
       [:td (? _ map?) (? ?date string?)]
       (?* _)]
      (?* _)]
     (?* _)]))

(defn parse-pos [p]
  (when (string? p)
    (parse-long (if (str/starts-with? p "=") (subs p 1) p))))

(def country-pat
  (pattern
   '[:tr (? _ map?)
     [:td (? _ map?) (? _ parse-pos)]
     (?* _)
     [:td (? _ map?) (? ?winner country-names)]
     [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
     (?* _)
     [:td (? _ map?) [:span (? _ map?) (?| ?medal ["Gold" "Silver" "Bronze"])]]
     (?* _)]))

(def winner-pat
  (pattern
   '[:a (?* _) (? ?winner string?)]))

(def accordian-pat
  (pattern
   '[:tr (? _ map?)
     [:td {:class "accordion-toggle"} ?_]
     [:td (? _ map?) (? _ parse-pos)]
     [:td (? _ map?) (? ?winner)]
     [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
     (?* _)
     [:td (? _ map?) [:span (? _ map?) (?| ?medal ["Gold" "Silver" "Bronze"])]]
     (?* _)]))

(def bib-pat-impl
  (pattern
   '[:tr (? _ map?)
     (?*? _)
     [:td (? _ map?) (? _ parse-pos)]
     (?*? _)
     [:td {:class "bib"} ??_]
     [:td (? _ map?) (?* ?winner)]
     [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
     (?* _)
     [:td (? _ map?) [:span (? _ map?) (?| medal ["Gold" "Silver" "Bronze"])]]
     (?* _)]))

(defn bib-pat [obj]
  (when-let [{:syms [?winner ?NOC] :as match} (bib-pat-impl obj)]
    (if (and (sequential? ?winner)
             (= "MIX" ?NOC))
      (when-let [winner-str (some->> ?winner
                                     (keep #('?winner (winner-pat %)))
                                     (seq)
                                     (str/join ", "))]
        (assoc match '?winner winner-str))
      match)))

(def player-pat
  (pattern
   '[:tr (? _ map?)
     [:td (? _ map?) (? _ parse-pos)]
     (?*? _)
     [:td (? _ map?) [:a (?* _) (? ?winner string?)]]
     (?*? _)
     [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
     (?* _)
     [:td (? _ map?) [:span (? _ map?) (?| medal ["Gold" "Silver" "Bronze"])]]
     (?* _)]))

(defn good-string?
  [obj]
  (and (string? obj)
       (let [s (str/replace obj #"\p{Pd}" "-")]
         (not= "-" s))))

(def string-pat
  (pattern
   '[:tr (? _ map?)
     [:td (? _ map?) (? _ parse-pos)]
     (?*? _)
     [:td (? _ map?) (? ?winner good-string?)]
     (?*? _)
     [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
     (?* _)
     [:td (? _ map?) [:span (? _ map?) (?| medal ["Gold" "Silver" "Bronze"])]]
     (?* _)]))

(def team-pat-impl
  (pattern
   '[:tr (? _ map?)
     [:td (? _ map?) (? _ parse-pos)]
     (?* _)
     [:td (? _ map?) (?* ?winner)]
     [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
     (?* _)
     [:td (? _ map?) [:span (? _ map?) (?| ?medal ["Gold" "Silver" "Bronze"])]]
     (?* _)]))

(defn team-pat [obj]
  (when-let [{:syms [?winner ?NOC] :as match} (team-pat-impl obj)]
    (when (and (sequential? ?winner) (seq ?winner))
      (if (= "MIX" ?NOC)
        (when-let [winner-str (some->> ?winner
                                       (keep #('?winner (winner-pat %)))
                                       (seq)
                                       (str/join ", "))]
          (assoc match '?winner winner-str))
        match))))

(def months->number
  {"January" "01"
   "February" "02"
   "March" "03"
   "April" "04"
   "May" "05"
   "June" "06"
   "July" "07"
   "August" "08"
   "September" "09"
   "October" "10"
   "November" "11"
   "December" "12"})

(defn format-date
  [?date]
  (try (-> ?date
         (str/replace #"\p{Pd}" "-")
         (str/replace #"\d+[ A-Za-z]* +- +(\d+ +[A-Za-z])" "$1")
         (str/replace #" +- +\d+:\d+" "")
         (->> (re-matches #".*?(\d+) +(\S*) +(\d\d\d\d).*"))
         ((fn [[_ day month year]]
            {:year (parse-long year)
             :month (parse-long (months->number month))
             :day (parse-long day)})))
    (catch Exception _
      {:year "XXX"
       :month "XX"
       :day "XX"})))

(comment
  (format-date "22 – 23 April 1906 — 17:00-,"))

(defn get-event-ids
  [pending-url]
  (let [[game-id sport-id event-id :as matches]
        (first (d/q '[:find ?g ?s ?e
                      :in $ ?event-url
                      :where
                      [?e :event/url ?event-url]
                      [?e :event/game ?g]
                      [?e :event/sport ?s]]
                 (d/db conn) (:url pending-url)))]
    {:game-id game-id
     :sport-id sport-id
     :event-id event-id}))

(comment
  (get-event-ids nil)
  (get-event-ids {:type :result
                  :url "/results/4004258",
                  :file "data/olympedia/results/19020750.edn"
                  :html (edn-read "data/olympedia/results/19020750.edn")})
  (d/q '[:find ?g ?s ?e
         :in $ ?event-url
         :where
         [?e :event/url ?event-url]
         [?e :event/game ?g]
         [?e :event/sport ?s]]
    (d/db conn) "/results/185062")
  )

(def statuses #{"Olympic" "Intercalated" "YOG"})

(def bad-files
  #{"data/olympedia/results/153156.edn" "data/olympedia/results/185098.edn" "data/olympedia/results/303000.edn"
    "data/olympedia/results/350939.edn" "data/olympedia/results/51499.edn" "data/olympedia/results/6000000.edn"
    "data/olympedia/results/6000242.edn" "data/olympedia/results/6000243.edn" "data/olympedia/results/920012.edn"
    "data/olympedia/results/920017.edn" "data/olympedia/results/920032.edn" "data/olympedia/results/920041.edn"
    "data/olympedia/results/920044.edn" "data/olympedia/results/920050.edn" "data/olympedia/results/920053.edn"
    "data/olympedia/results/920055.edn" "data/olympedia/results/920059.edn" "data/olympedia/results/920062.edn"
    "data/olympedia/results/920065.edn" "data/olympedia/results/920076.edn" "data/olympedia/results/923439.edn"
    "data/olympedia/results/923440.edn" "data/olympedia/results/924618.edn" "data/olympedia/results/9254.edn"
    "data/olympedia/results/964.edn"})

(defmethod get-links :result [pending-url]
  (prn :result (:url pending-url))
  (when-not (bad-files (:file pending-url))
    (let [html (or (:html pending-url) (parse-page (:url pending-url)))
          {:keys [game-id sport-id event-id]} (get-event-ids pending-url)
          status (volatile! nil)
          date (volatile! nil)
          rows (volatile! [])]
      (postwalk
       (fn [obj]
         (when-not @status
           (when-let [s (status-pat obj)]
             (vreset! status ('?status s))))
         (when-not @date
           (when-let [d (date-pat obj)]
             (vreset! date (format-date (or ('?date d) "")))))
         obj)
       html)
      (when (statuses @status)
        (postwalk
         (fn [obj]
           (when (vector? obj)
             (let [country-match (delay (country-pat obj))
                   accordian-match (delay (accordian-pat obj))
                   bib-match (delay (bib-pat obj))
                   player-match (delay (player-pat obj))
                   team-match (delay (team-pat obj))
                   string-match (delay (string-pat obj))]
               (when-let [{:syms [?winner ?NOC ?medal]} (or @country-match @accordian-match @bib-match
                                                            @player-match @team-match @string-match)]
                 (prn @country-match @accordian-match @bib-match
                      @player-match @team-match @string-match)
                 (let [winner (if (sequential? ?winner)
                                (->> ?winner
                                     (keep #(if (string? %) % ('?winner (winner-pat %))))
                                     (str/join ", "))
                                ?winner)
                       {:keys [year month day]} @date
                       new-row {:result/status @status
                                :result/year year
                                :result/month month
                                :result/day day
                                :result/winner winner
                                :result/country (country-code->name ?NOC)
                                :result/medal ?medal
                                :result/game game-id
                                :result/sport sport-id
                                :result/event event-id}]
                   (prn :new-row new-row)
                   (vswap! rows conj new-row)))))
           obj)
         html))
      (if (seq @rows)
        (d/transact! conn @rows)
        (when (statuses @status)
          (println "Cannot find match for" (pr-str (str base-url (:url pending-url)))
                   (pr-str (:file pending-url)))))
      nil)))

(defn make-url [file]
  (let [f-str (str file)]
    (subs f-str 14 (- (count f-str) 4))))

(defn reader []
  (get-links {:type :game
              :html (edn-read "data/olympedia/editions.edn")
              :url "/editions"})
  (doseq [file (fs/glob "data/olympedia/editions" "*.edn")]
    (get-links {:type :sport
                :html (edn-read file)
                :url (make-url file)}))
  (doseq [file (sort-by str (fs/match "data/olympedia/editions/" "regex:\\d+/sports/.*.edn" {:recursive true}))]
    (get-links {:type :event
                :file (str file)
                :html (edn-read file)
                :url (make-url file)}))
  (doseq [file (sort-by str (fs/glob "data/olympedia/results" "*.edn"))
          :let [html (edn-read file)
                pending-url {:type :result
                             :file (str file)
                             :html html
                             :url (make-url file)}]]
    (get-links pending-url)))

(comment
  (reader))

(defn get-rows []
  (into []
    (comp (mapcat identity)
      (map #(-> %
              (merge (:result/game %) (:result/sport %) (:result/event %))
              (dissoc :result/game :result/sport :result/event))))
    (d/q '[:find (pull ?e [:result/winner :result/country :result/medal
                           :result/year :result/month :result/day
                           {:result/game [:game/year :game/city]
                            :result/sport [:sport/name]
                            :result/event [:event/name :event/url]}])
           :where [?e :result/medal _]]
      (d/db conn))))

(comment
  (count (get-rows)) ; 24953
  (d/q '[:find (pull ?e [*])
         :where
         [?e :event/url "/results/4004258"]]
    (d/db conn))
  ,)

(defn set-season
  [row]
  (assoc row :game/season (season [(:game/year row) (:game/city row)])))

(defn clean-winner
  [row]
  (update row :result/winner #(-> %
                                (str/trim)
                                (str/replace ",  / ," ","))))

(defn strip-weightclass
  [row]
  (if (#{"Boxing" "Weightlifting" "Wrestling"} (:sport/name row))
    (-> row
      (update :event/name #(-> %
                              (str/trim)
                              (str/replace "&gt;" ">")
                              (str/replace "&lt;" "<")
                              (str/replace #" \(.[\d\.]+½? (pounds|kg|kilograms)\)," ",")
                              (str/replace #" \(.[\d\.]+½? lbs \[\d+ kg\]\)," ","))))
    row))

(comment
  (strip-weightclass {:sport/name "Boxing"
                      :event/name "Featherweight (≤60 kilograms), Men"})
  (set-season (last (get-rows))))

(def sorter
  (juxt :result/year #(format "%02d" (:result/month %)) #(format "%02d" (:result/day %))
        :sport/name :event/name #({"Gold" 1 "Silver" 2 "Bronze" 3} (:result/medal %)) :result/winner))

(def map->csv
  (juxt :game/season :result/year :result/month :result/day :game/city :sport/name :event/name
        :event/url :result/medal :result/winner :result/country))

(comment
  (let [ids (->> (d/q '[:find ?r
                        :in $ ?event-url
                        :where
                        [?e :event/url ?event-url]
                        [?r :result/event ?e]]
                      (d/db conn)
                      "/results/19006891")
                 (mapcat identity)
                 (mapv (fn [id] [:db.fn/retractEntity id])))]
    ; (prn ids)
    (d/transact! conn ids)
    ))

(comment
  (time (reader))
  (let [url "/results/19006891"
        file (io/file (str "data/olympedia" url ".edn"))
        html (edn-read file)
        pending-url {:type :result
                     :file (str file)
                     :html html
                     :url url}]
    (get-links pending-url))
  (with-open [writer (io/writer "./data/olympic-medals-2.csv")]
    (csv/write-csv writer
      (into [["season" "year" "month" "day" "city" "sport" "event" "url" "medal" "winner" "country"]]
            (->> (get-rows)
                 (mapv #(-> % set-season clean-winner strip-weightclass))
                 (sort-by sorter)
                 (distinct)
                 (mapv map->csv))))))
