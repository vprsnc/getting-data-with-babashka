#!/usr/bin/env bb

(deps/add-deps '{:deps {honeysql/honeysql {:mvn/version "1.0.444"}}})

(require '[babashka.pods :as pods]
         '[babashka.fs :as fs]
         '[clojure.java.io :as io]
         '[clojure.data.csv :as csv]
         '[honeysql.core :as sql]
         '[honeysql.helpers :as h])

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

(def insert
  (-> (h/insert-into :binance)
      (h/columns :currency :price :date)
      (h/values (vec (rest data)))
      sql/format))


(defn -main []
 (sqlite/execute! db-path insert))

(-main)

(comment
  (sqlite/query db-path "SELECT * FROM binance")
 )
