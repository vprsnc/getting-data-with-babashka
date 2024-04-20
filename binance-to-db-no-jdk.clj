#!/usr/bin/env bb

(require '[babashka.pods :as pods]
         '[babashka.fs :as fs]
         '[clojure.java.io :as io]
         '[clojure.data.csv :as csv])

(pods/load-pod 'org.babashka/go-sqlite3 "0.1.0")

(require '[pod.babashka.go-sqlite3 :as sqlite])


(def today
  (.format (java.text.SimpleDateFormat. "yyyy-MM-dd")
           (new java.util.Date)))

(def db-path (str (fs/cwd) "/data/" "data.db"))

(def csv-file-path (str (fs/cwd) "/data/binance-parsed/"
                        (format "binance-%s.csv" today)))

(def data (with-open [reader (io/reader csv-file-path)]
            (doall (csv/read-csv reader))))

(sqlite/execute!
 db-path
 "CREATE TABLE IF NOT EXISTS binance (currency TEXT, price DOUBLE, date TEXT)")




(defn -main []
  (for [d (rest data)]
    (sqlite/execute!
     db-path
     (vec
      (concat
       ["INSERT INTO binance (currency, price, date) VALUES (?, ?, ?)"]
       d)))))

(-main)

(comment
  (sqlite/query db-path "SELECT * FROM binance")
 )
