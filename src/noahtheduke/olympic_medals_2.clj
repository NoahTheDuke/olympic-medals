(ns noahtheduke.olympic-medals-2
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.walk :refer [postwalk]]
   [datalevin.core :as d]
   [hickory.core :refer [as-hiccup parse]]
   [noahtheduke.olympic-medals :as olympic-medals]
   [noahtheduke.olympic-medals.data :refer [country-code->name country-names
                                            season]]
   [noahtheduke.splint.pattern :refer [pattern]])
  (:import
   [clojure.lang ExceptionInfo]
   [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(def base-url
  "https://www.olympedia.org")

(defn parse-page
  [fragment]
  (-> (client/get (str base-url fragment))
      :body
      (parse)
      (as-hiccup)))

(def conn
  (d/get-conn "./db/dl.db"))

(def games
  [
   #:game{:url "/editions/1", :year "1896", :city "Athina"}
   #:game{:url "/editions/2", :year "1900", :city "Paris"}
   #:game{:url "/editions/3", :year "1904", :city "St. Louis"}
   #:game{:url "/editions/4", :year "1906", :city "Athina"}
   #:game{:url "/editions/5", :year "1908", :city "London"}
   #:game{:url "/editions/6", :year "1912", :city "Stockholm"}
   #:game{:url "/editions/7", :year "1920", :city "Antwerpen"}
   #:game{:url "/editions/8", :year "1924", :city "Paris"}
   #:game{:url "/editions/9", :year "1928", :city "Amsterdam"}
   #:game{:url "/editions/10", :year "1932", :city "Los Angeles"}
   #:game{:url "/editions/11", :year "1936", :city "Berlin"}
   #:game{:url "/editions/12", :year "1948", :city "London"}
   #:game{:url "/editions/13", :year "1952", :city "Helsinki"}
   #:game{:url "/editions/14", :year "1956", :city "Melbourne"}
   #:game{:url "/editions/15", :year "1960", :city "Roma"}
   #:game{:url "/editions/16", :year "1964", :city "Tokyo"}
   #:game{:url "/editions/17", :year "1968", :city "Ciudad de México"}
   #:game{:url "/editions/18", :year "1972", :city "München"}
   #:game{:url "/editions/19", :year "1976", :city "Montréal"}
   #:game{:url "/editions/20", :year "1980", :city "Moskva"}
   #:game{:url "/editions/21", :year "1984", :city "Los Angeles"}
   #:game{:url "/editions/22", :year "1988", :city "Seoul"}
   #:game{:url "/editions/23", :year "1992", :city "Barcelona"}
   #:game{:url "/editions/24", :year "1996", :city "Atlanta"}
   #:game{:url "/editions/25", :year "2000", :city "Sydney"}
   #:game{:url "/editions/26", :year "2004", :city "Athina"}
   #:game{:url "/editions/29", :year "1924", :city "Chamonix"}
   #:game{:url "/editions/30", :year "1928", :city "Sankt Moritz"}
   #:game{:url "/editions/31", :year "1932", :city "Lake Placid"}
   #:game{:url "/editions/32", :year "1936", :city "Garmisch-Partenkirchen"}
   #:game{:url "/editions/33", :year "1948", :city "Sankt Moritz"}
   #:game{:url "/editions/34", :year "1952", :city "Oslo"}
   #:game{:url "/editions/35", :year "1956", :city "Cortina d'Ampezzo"}
   #:game{:url "/editions/36", :year "1960", :city "Squaw Valley"}
   #:game{:url "/editions/37", :year "1964", :city "Innsbruck"}
   #:game{:url "/editions/38", :year "1968", :city "Grenoble"}
   #:game{:url "/editions/39", :year "1972", :city "Sapporo"}
   #:game{:url "/editions/40", :year "1976", :city "Innsbruck"}
   #:game{:url "/editions/41", :year "1980", :city "Lake Placid"}
   #:game{:url "/editions/42", :year "1984", :city "Sarajevo"}
   #:game{:url "/editions/43", :year "1988", :city "Calgary"}
   #:game{:url "/editions/44", :year "1992", :city "Albertville"}
   #:game{:url "/editions/45", :year "1994", :city "Lillehammer"}
   #:game{:url "/editions/46", :year "1998", :city "Nagano"}
   #:game{:url "/editions/47", :year "2002", :city "Salt Lake City"}
   #:game{:url "/editions/48", :year "1956", :city "Stockholm"}
   #:game{:url "/editions/49", :year "2006", :city "Torino"}
   #:game{:url "/editions/50", :year "1916", :city "Berlin"}
   #:game{:url "/editions/51", :year "1940", :city "Helsinki"}
   #:game{:url "/editions/52", :year "1944", :city "London"}
   #:game{:url "/editions/53", :year "2008", :city "Beijing"}
   #:game{:url "/editions/54", :year "2012", :city "London"}
   #:game{:url "/editions/55", :year "1940", :city "Garmisch-Partenkirchen"}
   #:game{:url "/editions/56", :year "1944", :city "Cortina d'Ampezzo"}
   #:game{:url "/editions/57", :year "2010", :city "Vancouver"}
   #:game{:url "/editions/58", :year "2014", :city "Sochi"}
   #:game{:url "/editions/59", :year "2016", :city "Rio de Janeiro"}
   #:game{:url "/editions/60", :year "2018", :city "PyeongChang"}
   #:game{:url "/editions/61", :year "2020", :city "Tokyo"}
   #:game{:url "/editions/62", :year "2022", :city "Beijing"}
   #:game{:url "/editions/63", :year "2024", :city "Paris"}
   #:game{:url "/editions/64", :year "2028", :city "Los Angeles"}
   #:game{:url "/editions/65", :year "2010", :city "Singapore"}
   #:game{:url "/editions/66", :year "2012", :city "Innsbruck"}
   #:game{:url "/editions/67", :year "2014", :city "Nanjing"}
   #:game{:url "/editions/68", :year "2016", :city "Lillehammer"}
   #:game{:url "/editions/69", :year "2018", :city "Buenos Aires"}
   #:game{:url "/editions/70", :year "2020", :city "Lausanne"}
   #:game{:url "/editions/71", :year "2026", :city "Dakar"}
   #:game{:url "/editions/72", :year "2026", :city "Milano-Cortina d'Ampezzo"}
   #:game{:url "/editions/73", :year "2024", :city "Gangwon"}
   #:game{:url "/editions/74", :year "1859", :city "Athina"}
   #:game{:url "/editions/75", :year "1870", :city "Athina"}
   #:game{:url "/editions/76", :year "1875", :city "Athina"}
   #:game{:url "/editions/77", :year "1889", :city "Athina"}
   #:game{:url "/editions/372", :year "2032", :city "Brisbane"}
   #:game{:url "/editions/373", :year "2030", :city "French Alps"}
   #:game{:url "/editions/374", :year "2034", :city "Salt Lake City, Utah"}
   #:game{:url "/editions/375", :year "2028", :city "Dolomiti Valtellina"}
   ]
  )

(comment
  (d/transact!
   conn
    games
   ))

(def games-by-url
  (->> (d/q
         '[:find (pull ?e [:db/id :game/url :game/year :game/city])
           :where
           [?e]]
         (d/db conn))
    (mapcat identity)
    (reduce (fn [acc cur] (assoc! acc (:game/url cur) cur)) (transient {}))
    (persistent!)))

(comment
  (->> (d/q
         '[:find (pull ?e [:db/id :game/url :game/year :game/city])
           :where
           [?e]]
         (d/db conn))
     (mapcat identity)
     (reduce (fn [acc cur] (assoc! acc (:game/url cur) cur)) (transient {}))
     (persistent!))
  (d/q '[:find ?e ?a ?v
         :where
         [?e ?a ?v]
         [?a :db/ident ?ident]
         #_[(namespace ?ident) ?ns]
         #_[(= ?ns "game")]]
    (d/db conn)))

(comment
  (first games-by-url)
  (first olympic-medals/sports))

(def discipline-pat
  (pattern
    '[:tr ?_
      "\n"
      [:td ?_ (? ?shortcode string?)]
      "\n"
      [:td ?_ [:a ?_ (? ?discipline string?)]]
      "\n"
      [:td ?_ [:a ?_ (? ?sport string?)]]
      "\n"
      [:td ?_ (?| ?season ["Summer" "Winter"])]
      ?*_]))

(defn add-key-ns
  [m ^String ns-str]
  (mapv (fn [d] (update-keys d (fn [k] (-> (name k)
                                         (subs 1)
                                         (->> (keyword ns-str))))))
    m))

(defn parse-sports []
  (let [html (parse-page "/sports")
        disciplines (volatile! [])]
    (postwalk
      (fn [obj]
        (when (vector? obj)
          (when-let [matches (discipline-pat obj)]
            (vswap! disciplines conj matches)))
        obj)
      html)
    (add-key-ns @disciplines "sport")))

(comment
  (parse-sports)
  #_(d/transact!
     conn
    (parse-sports)))

(def sports-by-url
  (into {}
    (for [sport olympic-medals/sports
          :let [segments (str/split (:sports/url sport) #"/")
                [[_ _editions game-id] [_sports sport-segs]] (split-at 3 segments)
                game (games-by-url (str "/editions/" game-id))
                sport' #:sport{:url (:sports/url sport)
                               :type (:sports/name sport)
                               :game (:db/id game)}]]
      (if game
        [(:sports/url sport) sport']
        (throw (ex-info "" {:sport sport
                            :segments segments}))))))

(comment
  (first sports-by-url)
  ,)

; (defn link-dispatch [pending-url] (:type pending-url))
;
; (defmulti get-links {:arglists '([pending-url])} #'link-dispatch)
; (defmethod get-links :default [pending-url] (throw (ex-info "default" pending-url)))
;
; (def game-pat
;   (pattern
;    '[:tr (?* _)
;      [:td (? _ map?) (?* _)]
;      "\n"
;      [:td (? _ map?) [:a {:href (? ?url string?)} (? ?year string?)]]
;      "\n"
;      [:td (? _ map?) [:a {:href (? ?url string?)} (? ?city string?)]]
;      (?* _)]))
;
; (defmethod get-links :games [pending-url]
;   (let [html (parse-page (:url pending-url))]
;     (postwalk
;      (fn [obj]
;        (when-let [{:syms [?year ?city ?url]} (game-pat obj)]
;          (let [row {:games/year ?year
;                     :games/city ?city
;                     :games/url ?url}]
;            (when (not= "Olympia" ?city)
;              (prn row)
;              (d/transact! conn row)
;              (d/transact! conn (merge pending-url new-row {:type :sports :url ?url}))
;              #_(let [new-row (-> (h/insert-into :games)
;                                (h/values [row])
;                                (h/returning :*)
;                                (execute!)
;                                (first))]
;                ))))
;        obj)
;      html)
;     html))
;
; (comment
;   (get-links {:type :games :url "/editions"}))
;
; (defn get-url
;   [url]
;   (and (string? url)
;        (re-find #"/editions/\d+/sports" url)))
;
; (def sport-pat
;   (pattern
;    '[:td
;      (?* _)
;      [:a {:href (? ?url get-url)} (? ?name string?)]
;      (?* _)]))
;
; (defmethod get-links :sports [pending-url]
;   (let [html (parse-page (:url pending-url))]
;     (postwalk
;      (fn [obj]
;        (when-let [{:syms [?url ?name]} (sport-pat obj)]
;          (let [row {:sports/url ?url
;                     :sports/name ?name
;                     :sports/game-id (-> pending-url :extra :games/id)}
;                new-row (-> (h/insert-into :sports)
;                            (h/values [row])
;                            (h/returning :*)
;                            (execute!)
;                            (first))]
;            (add-pending-url (merge pending-url new-row {:type :events :url ?url}))))
;        obj)
;      html)
;     nil))
;
; (comment
;   (get-links {:type :sport :url "/editions/1"}))
;
; (defn event-url
;   [?url]
;   (and (string? ?url)
;        (re-find #"/results/\d+" ?url)))
;
; (def event-pat
;   (pattern
;    '[:td (? _ map?) [:a {:href (? ?url event-url)} (? ?name string?)]]))
;
; (defmethod get-links :events [pending-url]
;   (let [html (parse-page (:url pending-url))]
;     (postwalk
;      (fn [obj]
;        (when-let [{:syms [?url ?name]} (event-pat obj)]
;          (let [row ?name
;                new-row (-> (h/insert-into :events)
;                            (h/values [{:events/url ?url
;                                        :events/name row
;                                        :events/game-id (-> pending-url :extra :games/id)
;                                        :events/sport-id (-> pending-url :extra :sports/id)}])
;                            (h/returning :*)
;                            (execute!)
;                            (first))]
;            (add-pending-url (merge pending-url new-row {:type :results :url ?url}))))
;        obj)
;      html)
;     nil))
;
; (comment
;   (get-links {:type :event
;               :sport/url "/editions/1/sports/WRE"}))
;
; (def status-pat
;   (pattern
;    '[:tr {} [:th {} "Status"] [:td {} ?status]]))
;
; (def date-pat
;   (pattern
;    '[:table {:class "biodata"} (?* _)
;      [:tbody (? _ map?) (?* _)
;       [:tr (? _ map?)
;        (?* _)
;        [:th (? _ map?) "Date"]
;        (?* _)
;        [:td (? _ map?) (? ?date string?)]
;        (?* _)]
;       (?* _)]
;      (?* _)]))
;
; (defn parse-pos [p]
;   (when (string? p)
;     (parse-long (if (str/starts-with? p "=") (subs p 1) p))))
;
; (def team-pat
;   (pattern
;    '[:tr (? _ map?)
;      [:td (? _ map?) (? _ parse-pos)]
;      (?* _)
;      [:td (? _ map?) (? ?winner country-names)]
;      [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
;      (?* _)
;      [:td (? _ map?) [:span (? _ map?) (?| ?medal ["Gold" "Silver" "Bronze"])]]
;      (?* _)]))
;
; (def accordian-pat
;   (pattern
;    '[:tr (? _ map?)
;      [:td {:class "accordion-toggle"} ?_]
;      [:td (? _ map?) (? _ parse-pos)]
;      [:td (? _ map?) (? ?winner)]
;      [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
;      (?* _)
;      [:td (? _ map?) [:span (? _ map?) (?| ?medal ["Gold" "Silver" "Bronze"])]]
;      (?* _)]))
;
; (def bib-pat
;   (pattern
;    '[:tr (? _ map?)
;      (?*? _)
;      [:td (? _ map?) (? _ parse-pos)]
;      (?*? _)
;      [:td {:class "bib"} ?_]
;      [:td (? _ map?) (?* ?winner)]
;      [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
;      (?* _)
;      [:td (? _ map?) [:span (? _ map?) (?| medal ["Gold" "Silver" "Bronze"])]]
;      (?* _)]))
;
; (def player-pat
;   (pattern
;    '[:tr (? _ map?)
;      [:td (? _ map?) (? _ parse-pos)]
;      (?*? _)
;      [:td (? _ map?) [:a (?* _) (? ?winner string?)]]
;      (?*? _)
;      [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
;      (?* _)
;      [:td (? _ map?) [:span (? _ map?) (?| medal ["Gold" "Silver" "Bronze"])]]
;      (?* _)]))
;
; (def string-pat
;   (pattern
;    '[:tr (? _ map?)
;      [:td (? _ map?) (? _ parse-pos)]
;      (?*? _)
;      [:td (? _ map?) (? ?winner string?)]
;      (?*? _)
;      [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
;      (?* _)
;      [:td (? _ map?) [:span (? _ map?) (?| medal ["Gold" "Silver" "Bronze"])]]
;      (?* _)]))
;
; (def winner-pat
;   (pattern
;    '[:a (?* _) (? ?winner string?)]))
;
; (def months->number
;   {"January" "01"
;    "February" "02"
;    "March" "03"
;    "April" "04"
;    "May" "05"
;    "June" "06"
;    "July" "07"
;    "August" "08"
;    "September" "09"
;    "October" "10"
;    "November" "11"
;    "December" "12"})
;
; (defn format-date
;   [?date]
;   (-> ?date
;       (str/replace #"\p{Pd}" "-")
;       (str/replace #"\d+[ A-Za-z]* +- +(\d+ +[A-Za-z])" "$1")
;       (str/replace #" +- +\d+:\d+" "")
;       (->> (re-matches #".*?(\d+) +(\S*) +(\d\d\d\d)"))
;       ((fn [[_ day month year]]
;          (format "%04d-%02d-%02d"
;                  (parse-long year)
;                  (parse-long (months->number month))
;                  (parse-long day))))))
;
; (comment
;   (format-date "2 –  7 July 1904"))
;
; (defn get-event-ids
;   [pending-url]
;   (let [existing-row (-> (h/select :*)
;                          (h/from :events)
;                          (h/where [:= :url (:url pending-url)])
;                          (h/limit 1)
;                          (execute!)
;                          (first))
;         game-id (or (-> pending-url :extra :games/id)
;                     (:events/game-id existing-row))
;         sport-id (or (-> pending-url :extra :sports/id)
;                      (:events/sport-id existing-row))
;         event-id (or (-> pending-url :extra :events/id)
;                      (:events/id existing-row))]
;     {:game-id game-id
;      :sport-id sport-id
;      :event-id event-id}))
;
; (comment
;   (get-event-ids {:type :results
;                   :url "/results/9616"}))
;
; (defmethod get-links :results [pending-url]
;   (let [html (parse-page (:url pending-url))
;         {:keys [game-id sport-id event-id]} (get-event-ids pending-url)
;         status (volatile! nil)
;         date (volatile! nil)
;         rows (volatile! [])
;         insert (fn [row]
;                  (prn row)
;                  (vswap! rows conj row)
;                  (try (-> (h/insert-into :results)
;                      (h/values [row])
;                      (h/returning :*)
;                      (execute!))
;                       (catch Exception ex
;                         (prn (ex-message ex)))))]
;     (postwalk
;      (fn [obj]
;        (when-not @status
;          (when-let [s (status-pat obj)]
;            (vreset! status ('?status s))))
;        (when-not @date
;          (when-let [d (date-pat obj)]
;            (vreset! date (format-date ('?date d "")))))
;        obj)
;      html)
;     (if (not= "Olympic" @status)
;       (insert {:results/status @status})
;       (postwalk
;        (fn [obj]
;          (when (vector? obj)
;            (let [team-match (delay (team-pat obj))
;                  accordian-match (delay (accordian-pat obj))
;                  bib-match (delay (bib-pat obj))
;                  player-match (delay (player-pat obj))
;                  string-match (delay (string-pat obj))]
;              (when-let [{:syms [?winner ?NOC ?medal]} (or @team-match @accordian-match @bib-match
;                                                           @player-match @string-match)]
;                (let [winner (if (sequential? ?winner)
;                               (->> ?winner
;                                    (keep #(if (string? %) % ('?winner (winner-pat %))))
;                                    (str/join ", "))
;                               ?winner)]
;                  (insert {:results/status @status
;                           :results/date @date
;                           :results/athlete winner
;                           :results/country (country-code->name ?NOC)
;                           :results/medal ?medal
;                           :results/game-id game-id
;                           :results/sport-id sport-id
;                           :results/event-id event-id})))))
;          obj)
;        html))
;     html))
;
; (comment
;   (get-links {:type :results
;               :url "/results/9616"}))
;
; (defn executor []
;   (loop []
;     (when-let [link (get-pending-url)]
;       (prn (:type link) (:url link))
;       (try (when-not (seen-url? (:url link))
;              (saw-url (:url link))
;              (get-links link))
;            (catch ExceptionInfo ex
;              (if (= 429 (:status (ex-data ex)))
;                (do (add-pending-url link)
;                    (prn "sleeping")
;                    (.sleep TimeUnit/SECONDS 45))
;                (do (prn ex)
;                    (throw ex)))))
;       (recur))))
;
; (comment
;   (add-pending-url {:type :games :url "/editions"})
;   (executor))
;
; (defn get-rows []
;   (-> (h/select :games/year :games/city :sports/name :events/name :events/url
;                 :results/date :results/athlete :results/country :results/medal)
;       (h/from :results)
;       (h/join :games [:= :games/id :results/game-id])
;       (h/join :sports [:= :sports/id :results/sport-id])
;       (h/join :events [:= :events/id :results/event-id])
;       (execute!)))
;
; (comment
;   (last (get-rows)))
;
; (defn set-date
;   [row]
;   (let [[_ year month day] (re-find #".*?(\d\d\d\d)-(\d\d)-(\d+)" (:results/date row))]
;     (-> row
;         (assoc :results/year (parse-long year))
;         (assoc :results/month (parse-long month))
;         (assoc :results/day (parse-long day))
;         (assoc :results/date (format "%s-%s-%02d" year month (parse-long day))))))
;
; (comment
;   country-names
;   (set-date {:results/date "8 -  2026-02-9"}))
;
; (defn set-season
;   [row]
;   (assoc row :games/season (season [(:games/year row) (:games/city row)])))
;
; (comment
;   (set-season (last (get-rows))))
;
; (def map->csv
;   (juxt :games/season :results/year :results/month :results/day :games/city :sports/name :events/name
;         :events/url :results/medal :results/athlete :results/country))
;
; (def sorter
;   (juxt :results/year #(format "%02d" (:results/month %)) #(format "%02d" (:results/day %))
;         :sports/name :events/name #({"Gold" 1 "Silver" 2 "Bronze" 3} (:results/medal %)) :results/athlete))
;
; (comment
;   (with-open [writer (io/writer "./data/olympic-medals-2.csv")]
;     (csv/write-csv writer
;       (into [["season" "year" "month" "day" "city" "sport" "event" "url" "medal" "winner" "country"]]
;             (->> (get-rows)
;                  (mapv #(-> % set-date set-season))
;                  (sort-by sorter)
;                  (mapv map->csv))))))
;
; (defn csv-data->maps [csv-data]
;   (map zipmap
;        (->> (first csv-data)
;             (map (comp keyword str/lower-case))
;             repeat)
; 	  (rest csv-data)))
;
; (defonce v1
;   (with-open [reader (io/reader "./data/olympic-medals.csv")]
;     (->> (csv/read-csv reader)
;          (csv-data->maps)
;          (mapv #(-> %
;                     (update :year parse-long)
;                     (update :month parse-long)
;                     (update :day parse-long))))))
;
; (defonce existing-urls
;   (->> v1
;        (mapv :url)
;        (set)))
;
; (defonce empty-urls
;   (-> (h/select :*)
;       (h/from :events)
;       (h/where [:not-in :url existing-urls])
;       (execute!)
;       (->> (mapv :events/url))
;       (set)))
;
; (prn empty-urls)
;
; (def v4
;   (group-by (juxt :season :year :month :day :city :sport :event :url :medal :country)
;             v1))
;
; (def temp
;   (->> (vals v4)
;        (mapcat (fn [row]
;                  (if (= 1 (count row))
;                    row
;                    (remove #(#{"" "-" "–"} (:winner %)) row))))))
;
; (defn set-by [f coll]
;   (persistent!
;    (reduce
;     (fn [ret x]
;       (let [k (f x)]
;         (assoc! ret k (conj (get ret k #{}) x))))
;     (transient {}) coll)))
;
; (def v3
;   (set-by (juxt :url :medal) v1))
;
; (def v3->csv
;   (juxt :season :year :month :day :city :sport :event :url :medal :winner :country))
;
; (def v3-sorter
;   (juxt :year #(format "%02d" (:month %)) #(format "%02d" (:day %))
;         :sport :event #({"Gold" 1 "Silver" 2 "Bronze" 3} (:medal %)) :winner))
;
; (comment
;   (with-open [writer (io/writer "./data/olympic-medals-2.csv")]
;     (csv/write-csv writer
;       (into [["season" "year" "month" "day" "city" "sport" "event" "url" "medal" "winner" "country"]]
;             (->> temp
;                  (sort-by v3-sorter)
;                  (mapv v3->csv))))))
