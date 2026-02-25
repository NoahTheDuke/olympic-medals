(ns noahtheduke.olympic-medals.html-parser
  (:require
   [clj-http.client :as client]
   [clojure.walk :refer [postwalk]]
   [hickory.core :refer [as-hiccup parse]]))

(def base-url "https://www.olympedia.org")

(defn parse-page
  [fragment]
  (-> (client/get (str base-url fragment))
      :body
      (parse)
      (as-hiccup)))

(defn traverse
  [obj f]
  (postwalk #(doto % f) obj)
  obj)

(comment
  (let [nums (atom [])]
    (traverse {:a [1 2 3]}
      (fn [obj]
        (when (number? obj)
          (swap! nums conj obj))))
    @nums)
  )
