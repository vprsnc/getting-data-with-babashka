#!/usr/bin/env bb

(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io]
         '[babashka.fs :as fs])

(def today
  (.format (java.text.SimpleDateFormat. "yyyy-MM-dd")
           (new java.util.Date)))

(def input-directory (str (fs/cwd) (format "/data/binance-edn/%s/" today)))
(def output-directory (str (fs/cwd) "/data/binance-parsed/"))
(def output-filename (format "binance-%s.csv" today))

(def binance-atom (atom {:data []}))

(def files (fs/list-dir input-directory))

(defn parse-edn [file]
  (-> (str file)
      ((comp :data read-string slurp))))


(defn parse-single-price 
  [{:keys [adv]}]
  (let [price {:currency (:fiatUnit adv)
               :price (:price adv)}]
       (swap! binance-atom update-in [:data] conj price)
    price))


(comment
  (-> (second files)
      append-prices)

  @binance-atom
  (reset! binance-atom {:data []})
  )

(defn append-prices [file]
  (->> (parse-edn file)
       (pmap parse-single-price))
  nil)

(defn write-csv [path row-data]
  (let [columns (into [] (keys (first row-data)))
        headers (map name columns)
        rows (mapv #(mapv % columns) row-data)]
    (with-open [file (io/writer path)]
      (csv/write-csv file (cons headers rows)))))

(defn -main 
  []
  (when (not (fs/directory? output-directory))
    (fs/create-dir output-directory))
  (doall (map append-prices files))
  (->> (map #(assoc % :date today) (:data @binance-atom))
       (write-csv (str output-directory output-filename))))

(-main)
