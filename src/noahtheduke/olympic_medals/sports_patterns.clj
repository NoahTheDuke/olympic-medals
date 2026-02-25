(ns noahtheduke.olympic-medals.sports-patterns
  (:require
   [clojure.string :as str]
   [noahtheduke.olympic-medals.html-parser :refer [parse-page]]
   [pattern :refer [compile-pattern]]))

(defn parse-pos [p]
  (when (string? p)
    (parse-long (if (str/starts-with? p "=") (subs p 1) p))))

(def sport-patterns (atom {}))

(def date-pat
  (compile-pattern
   '[:table {:class "biodata"} (?:* _)
     [:tbody (? _ map?) (?:* _)
      [:tr (? _ map?)
       (?:* _)
       [:th (? _ map?) "Date"]
       (?:* _)
       [:td (? _ map?) (? date string?)]
       (?:* _)]
      (?:* _)]
     (?:* _)]))

(def table
  [:table
     {:class "table table-striped"}
     [:thead
      {}
      [:tr
       {}
       [:th {} "Pos"]
       [:th {} "Number"]
       [:th {} "Competitor"]
       [:th {} "NOC"]
       [:th {} "R1"]
       [:th {} "QF"]
       [:th {} "SF"]
       [:th {} "Final"]
       [:th {}]
       [:th {}]
       [:th {}]]]
     [:tbody
      {}
      [:tr
       {:class "", :style ""}
       [:td {} "1"]
       [:td {:class "bib"} "135"]
       [:td {} [:a {:href "/athletes/136538"} "Sándor Tótka"]]
       [:td
        {}
        [:a
         {:href "/countries/HUN"}
         [:img
          {:style "padding-right: 2px; vertical-align: middle",
           :src
           "https://olympedia-flags.s3.eu-central-1.amazonaws.com/HUN.png"}]
         "HUN"]]
       [:td {} "35.070 (1 h3)"]
       [:td {} "–"]
       [:td {} "35.114 (1 h2)"]
       [:td {} "35.035 (1 h1)"]
       [:td {} [:span {:class "Gold"} "Gold"]]
       [:td {}]
       [:td {}]]
      [:tr
       {:class "", :style ""}
       [:td {} "2"]
       [:td {:class "bib"} "150"]
       [:td {} [:a {:href "/athletes/134602"} "Manfredi Rizza"]]
       [:td
        {}
        [:a
         {:href "/countries/ITA"}
         [:img
          {:style "padding-right: 2px; vertical-align: middle",
           :src
           "https://olympedia-flags.s3.eu-central-1.amazonaws.com/ITA.png"}]
         "ITA"]]
       [:td {} "34.867 (1 h4)"]
       [:td {} "–"]
       [:td {} "35.171 (2 h2)"]
       [:td {} "35.080 (2 h1)"]
       [:td {} [:span {:class "Silver"} "Silver"]]
       [:td {}]
       [:td {}]]]])

(defn make-rows [{:keys [position winner NOC medal]}]
  (mapv (fn [p w n m] {:position p :winner w :NOC n :medal m})
    position
    winner
    NOC
    medal))

(def csp-canoe-sprint
  (compile-pattern
    '[:table (? _ map?)
      [:thead (?:* (? foo))]
      [:tbody
       (? _ map?)
       (?:as* rows
         (?:*
           [:tr (? _ map?)
            [:td (? _ map?) (? position parse-pos)]
            (?? _)
            [:td (? _ map?) [:a (?? _) (? winner string?)]]
            (?? _)
            [:td (? _ map?) [:a (?? _) (? NOC country-code->name)]]
            (?? _)
            [:td (? _ map?) [:span (? _ map?) (? medal #{"Gold" "Silver" "Bronze"})]]
            (?? _)]))]]))

(make-rows (csp-canoe-sprint table))

(comment
  (def html (parse-page "/results/19016100"))
  (do html)
  )
