(ns noahtheduke.olympic-medals
  (:require
   [clj-http.client :as client]
   [clojure.data.csv :as csv]
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.walk :refer [postwalk]]
   [hickory.core :refer [as-hiccup parse]]
   [honey.sql :as sql]
   [honey.sql.helpers :as h]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [noahtheduke.olympic-medals.data :refer [country-code->name country-names
                                            season]]
   [noahtheduke.splint.pattern :refer [pattern]])
  (:import
   [clojure.lang ExceptionInfo]
   [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(defn json->str [json]
  (json/read-str json {:key-fn keyword}))

(defn str->json [s]
  (json/write-str s {:key-fn (fn [k]
                               (if (or (symbol? k) (keyword? k))
                                 (str (namespace k) (when (namespace k) "/") (name k))
                                 (str k)))}))

(def ds (jdbc/get-datasource "jdbc:sqlite:db/database.db"))

(defn sql-fmt
  ([stmt] (sql-fmt stmt nil))
  ([stmt {:keys [debug] :as opts}]
   (let [qs (sql/format stmt)]
     (when debug
       (prn qs))
     qs)))

(defn execute!
  ([stmt] (execute! stmt nil))
  ([stmt opts]
   (let [qs (sql-fmt stmt opts)]
     (jdbc/execute! ds qs {:return-keys true
                           :builder-fn rs/as-kebab-maps}))))

(-> (h/create-table :games :if-not-exists)
    (h/with-columns
      [:id :integer [:primary-key] :autoincrement [:not nil]]
      [:url :text]
      [:year :text]
      [:city :text])
    (execute!))
(-> (h/create-table :sports :if-not-exists)
    (h/with-columns [:id :integer [:primary-key] :autoincrement [:not nil]]
      [:url :text]
      [:name :text]
      [:game-id :integer [:not nil]]
      [[:foreign-key :game-id] :references [:games :id]])
    (execute!))
(-> (h/create-table :events :if-not-exists)
    (h/with-columns
      [:id :integer [:primary-key] :autoincrement [:not nil]]
      [:url :text]
      [:name :text]
      [:game-id :integer [:not nil]]
      [:sport-id :integer [:not nil]]
      [[:foreign-key :game-id] :references [:games :id]]
      [[:foreign-key :sport-id] :references [:sports :id]])
    (execute!))
(-> (h/create-table :results :if-not-exists)
    (h/with-columns
      [:id :integer [:primary-key] :autoincrement [:not nil]]
      [:date :text]
      [:athlete :text]
      [:country :text]
      [:medal :text]
      [:game-id :integer [:not nil]]
      [:sport-id :integer [:not nil]]
      [:event-id :integer [:not nil]]
      [[:foreign-key :game-id] :references [:games :id]]
      [[:foreign-key :sport-id] :references [:sports :id]]
      [[:foreign-key :event-id] :references [:events :id]])
    (execute!))

(-> (h/create-table :pending-urls :if-not-exists)
    (h/with-columns
      [:id :integer [:primary-key] :autoincrement [:not nil]]
      [:type :text]
      [:url :text]
      [:extra :json])
    (execute!))

(-> (h/create-table :seen-urls :if-not-exists)
    (h/with-columns [:url :text])
    (execute!))

(defn add-pending-url [{:keys [type url extra] :as row}]
  (-> (h/insert-into :pending-urls)
      (h/values [{:type (name type)
                  :url url
                  :extra (str->json (-> row
                                        (dissoc :type :url :extra)
                                        (merge extra)))}])
      (execute!)))

(comment
  (add-pending-url {:type :foo
                    :url "hello world"
                    :extra {:cool :stuff}}))

(defn get-pending-url []
  (let [row (-> (h/select :*)
                (h/from :pending-urls)
                (h/order-by [:id :desc])
                (h/limit 1)
                (execute!)
                (first)
                (some->
                 (-> (update-keys #(keyword (name %)))
                     (update :type keyword)
                     (update :extra json->str))))]
    (when row
      (-> (h/delete-from :pending-urls)
          (h/where [:= :url (:url row)])
          (execute!))
      row)))

(comment
  (get-pending-url))

(defn seen-url? [url]
  (let [ret (-> (h/select :url)
      (h/from :seen-urls)
      (h/where [:= :url url])
      (h/limit 1)
      (execute!))]
    (some? (seq ret))))

(defn saw-url [url]
  (-> (h/insert-into :seen-urls)
      (h/values [{:url url}])
      (execute!))
  nil)


(defn link-dispatch [pending-url] (:type pending-url))

(defmulti get-links {:arglists '([pending-url])} #'link-dispatch)
(defmethod get-links :default [pending-url] (throw (ex-info "default" pending-url)))

(def base-url
  "https://www.olympedia.org")

(defn parse-page
  [fragment]
  (-> (client/get (str base-url fragment))
      :body
      (parse)
      (as-hiccup)))

(def game-pat
  (pattern
   '[:tr (?* _)
     [:td (? _ map?) (?* _)]
     "\n"
     [:td (? _ map?) [:a {:href (? ?url string?)} (? ?year string?)]]
     "\n"
     [:td (? _ map?) [:a {:href (? ?url string?)} (? ?city string?)]]
     (?* _)]))

(defmethod get-links :games [pending-url]
  (let [html (parse-page (:url pending-url))]
    (postwalk
     (fn [obj]
       (when-let [{:syms [?year ?city ?url]} (game-pat obj)]
         (let [row {:games/year ?year
                    :games/city ?city
                    :games/url ?url}]
           (when (not= "Olympia" ?city)
             (let [new-row (-> (h/insert-into :games)
                               (h/values [row])
                               (h/returning :*)
                               (execute!)
                               (first))]
               ; (prn :games/new-row new-row)
               (add-pending-url (merge pending-url new-row {:type :sports :url ?url}))))))
       obj)
     html)
    nil))

(comment
  (get-links {:type :games :url "/editions"}))

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

(defmethod get-links :sports [pending-url]
  (let [html (parse-page (:url pending-url))]
    (postwalk
     (fn [obj]
       (when-let [{:syms [?url ?name]} (sport-pat obj)]
         (let [row {:sports/url ?url
                    :sports/name ?name
                    :sports/game-id (-> pending-url :extra :games/id)}
               new-row (-> (h/insert-into :sports)
                           (h/values [row])
                           (h/returning :*)
                           (execute!)
                           (first))]
           ; (prn :sports/new-row new-row)
           (add-pending-url (merge pending-url new-row {:type :events :url ?url}))))
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

(defmethod get-links :events [pending-url]
  (let [html (parse-page (:url pending-url))]
    (postwalk
     (fn [obj]
       (when-let [{:syms [?url ?name]} (event-pat obj)]
         (let [row ?name
               new-row (-> (h/insert-into :events)
                           (h/values [{:events/url ?url
                                       :events/name row
                                       :events/game-id (-> pending-url :extra :games/id)
                                       :events/sport-id (-> pending-url :extra :sports/id)}])
                           (h/returning :*)
                           (execute!)
                           (first))]
           ; (prn :events/new-row new-row)
           (add-pending-url (merge pending-url new-row {:type :results :url ?url}))))
       obj)
     html)
    nil))

(comment
  (get-links {:type :event
              :sport/url "/editions/1/sports/WRE"}))

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

(def athlete-pat
  (pattern '[:a {:href ?athlete-url} ?name]))

(def position-pat
  (pattern
   '[:tr (? _ map?)
     [:td (? _ map?) ?position]
     (?* _)
     [:td (? _ map?) (?+ ?players)]
     [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
     (?* _)
     [:td (? _ map?) [:span (? _ map?) (?| medal ["Gold" "Silver" "Bronze"])]]
     (?* _)]))

(def team-pat
  (pattern
   '[:tr (? _ map?)
     [:td (? _ map?) ?position]
     (?* _)
     [:td (? _ map?) (? ?team country-names)]
     [:td (? _ map?) [:a (?* _) (? ?NOC country-code->name)]]
     (?* _)
     [:td (? _ map?) [:span (? _ map?) (?| ?medal ["Gold" "Silver" "Bronze"])]]
     (?* _)]))

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
  (-> ?date
      (str/replace #"\p{Pd}" "-")
      (str/replace #"\d+ - (\d+ [A-Za-z])" "$1")
      (str/replace #" - \d+:\d+" "")
      (str/replace #"(\d+) (\S*) (\d\d\d\d)"
                   (fn [[_ day month year]]
                     (str year "-" (months->number month) "-" day)))))

(comment
  (format-date "8 – 9 February 2026"))

(defmethod get-links :results [pending-url]
  (let [html (parse-page (:url pending-url))
        date (volatile! nil)
        insert (fn [row]
                 (-> (h/insert-into :results)
                     (h/values [row])
                     (h/returning :*)
                     (execute!)))]
    (postwalk
     (fn [obj]
       (when-not @date
         (when-let [d (date-pat obj)]
           (vreset! date (format-date ('?date d)))))
       obj)
     html)
    (postwalk
     (fn [obj]
       (if-let [{:syms [?team ?NOC ?medal]} (team-pat obj)]
         (insert {:results/date @date
                  :results/athlete ?team
                  :results/country (country-code->name ?NOC)
                  :results/medal ?medal
                  :results/game-id (-> pending-url :extra :games/id)
                  :results/sport-id (-> pending-url :extra :sports/id)
                  :results/event-id (-> pending-url :extra :events/id)})
         (when-let [{:syms [?players ?NOC ?medal]} (position-pat obj)]
           (let [athletes (keep athlete-pat ?players)]
             (insert {:results/date @date
                      :results/athlete (if (= 1 (count athletes))
                                         ('?name (first athletes))
                                         (str/join ", " (map '?name athletes)))
                      :results/country (country-code->name ?NOC)
                      :results/medal ?medal
                      :results/game-id (-> pending-url :extra :games/id)
                      :results/sport-id (-> pending-url :extra :sports/id)
                      :results/event-id (-> pending-url :extra :events/id)}))))
       obj)
     html)
    html))

(comment
  (get-links {:type :results
              :url "/results/9009310"}))

(defn executor []
  (add-pending-url {:type :games :url "/editions"})
  (loop []
    (when-let [link (get-pending-url)]
      (prn (:type link) (:url link))
      (try (when-not (seen-url? (:url link))
             (saw-url (:url link))
             (get-links link))
           (catch ExceptionInfo ex
             (if (= 429 (:status (ex-data ex)))
               (do (add-pending-url link)
                   (prn "sleeping")
                   (.sleep TimeUnit/SECONDS 30))
               (do (prn ex)
                   (throw ex)))))
      (recur))))

(comment
  (executor))

"1 August 2021"


(defn get-rows []
  (-> (h/select :games/year :games/city :sports/name :events/name :events/url
                :results/date :results/athlete :results/country :results/medal)
      (h/from :results)
      (h/join :games [:= :games/id :results/game-id])
      (h/join :sports [:= :sports/id :results/sport-id])
      (h/join :events [:= :events/id :results/event-id])
      (execute!)
      ))

(comment
  (last (get-rows)))

(defn set-date
  [row]
  (let [[_ year month day] (re-find #".*?(\d\d\d\d)-(\d\d)-(\d+)" (:results/date row))]
    (-> row
        (assoc :results/year (parse-long year))
        (assoc :results/month (parse-long month))
        (assoc :results/day (parse-long day))
        (assoc :results/date (format "%s-%s-%02d" year month (parse-long day))))))

(comment
  country-names
  (set-date {:results/date "8 -  2026-02-9"}))

(defn set-season
  [row]
  (assoc row :games/season (season [(:games/year row) (:games/city row)])))

(comment
  (set-season (last (get-rows))))

(def map->csv
  (juxt :games/season :results/year :results/month :results/day :games/city :sports/name :events/name
        :events/url :results/medal :results/athlete :results/country))

(def rows (get-rows))
(comment
  (->> rows
       (take 1)
       (mapv #(-> % set-date set-season))
       (sort-by (juxt :results/date :sports/name :events/name #({"Gold" 1 "Silver" 2 "Bronze" 3} (:results/medal %))))
       (mapv map->csv))
  (with-open [writer (io/writer "./data/olympic-medals-2.csv")]
    (csv/write-csv writer
      (into [["season" "year" "month" "day" "city" "sport" "event" "url" "medal" "winner" "country"]]
            (->> (get-rows)
                 (mapv #(-> % set-date set-season))
                 (sort-by (juxt :results/date :sports/name :events/name #({"Gold" 1 "Silver" 2 "Bronze" 3} (:results/medal %))))
                 (mapv map->csv))))))
