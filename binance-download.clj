#!/usr/bin/env bb

(require '[babashka.curl :as curl]
         '[clojure.walk :as walk]
         '[cheshire.core :as json]
         '[clojure.string]
         '[babashka.fs :as fs]
         '[clojure.math :refer [ceil]])

(def currencies 
  ["ARS" "EUR" "USD" "AED" "AUD" "BDT" "BHD" "BIF" "BOB" "BRL" "CAD"
   "CLP" "CNY" "COP" "CRC" "CZK" "DOP" "DZD" "EGP" "GBP" "GEL" "GHS"
   "HKD" "IDR" "INR" "JPY" "KES" "KHR" "KRW" "KWD" "KZT" "LAK" "LBP"
   "LKR" "MAD" "MMK" "MXN" "MYR" "NGN" "OMR" "SGD" "PAB" "PEN" "PHP"
   "PKR" "PLN" "PYG" "QAR" "RON" "ETB" "SAR" "SDG" "SEK" "SGD" "THB"
   "TND" "TRY" "IQD" "TWD" "UAH" "UGX" "UYU" "VES" "VND" "ZAR" "NPR"
   "UZS" "BDT" "XOF" "SOS" "MNT" "IRR" "MAD" "XAF" "LKR" "PKR" "AUD"
   "BGN" "BRL" "CHF" "CLP" "COP" "CRC" "CZK" "DKK" "HUF" "ILS" "ISK"
   "JOD" "JPY" "KES" "KRW" "MYR" "NGN" "NOK" "NZD" "PLN" "QAR" "RON"
   "RSD" "SEK" "SGD" "THB" "TND" "TRY" "TWD" "ZAR" "BGN" "CHF" "CRC"
   "DKK" "HUF" "ILS" "ISK" "KRW" "MYR" "NOK" "NZD" "RON" "RSD" "SEK"])

(def today
  (.format (java.text.SimpleDateFormat. "yyyy-MM-dd")
           (new java.util.Date)))


(defn generate-payload
  [curr page]
  {:page page
   :rows 20
   :asset "USDT"
   :tradeType "BUY"
   :fiat curr
   :merchantCheck true})


(defn get-request
  [curr page]
  (let [response (curl/post
                  "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
                  {:throw false  ;; not to throw exception when status>400
                   :headers {:content-type "application/json"
                             :accept "application/json"}
                   :body (json/encode (generate-payload curr page))})
        status (:status response)
        status=? (partial = status)]
    (cond (status=? 200) response
          (status=? 429) (do
                           (println "Sleeping 3s...")
                           (Thread/sleep 3000)
                           (recur curr page))
          :else (do (println "Something went wrong...")
                    (println (:err response))))))


(defn parse-response
  [resp]
  (-> (:body resp)
      json/parse-string
      clojure.walk/keywordize-keys))


(defn get-total-entries
  [{:keys [total]}]
  total)


(defn write-page-edn
  [filepath page-edn]
  (let
   [directory (->> (clojure.string/split filepath #"/")
                   rest
                   (into [])
                   drop-last
                   (clojure.string/join "/")
                   (str "/"))]
    (when (not (fs/directory? directory))
      (fs/create-dir directory))
    (spit filepath (with-out-str (pr page-edn)))
    nil))


(defn get-all-pages
  [path currency]
  (println (format "Getting %s" currency))
  (let [resp (get-request currency 1)
        resp-body (parse-response resp)
        total-entries (get-total-entries resp-body)
        pages (int (ceil (/ total-entries 20)))
        get-this-currency (partial get-request currency)]
    (for [p (range 1 (inc pages))]
      (->> (get-this-currency p)
           parse-response
           (write-page-edn
            (format "%s/data/binance-edn/%s/%s-%d.edn"
                    path today currency p))))))

(defn -main
  []
  (println "Starting the ETL...\n")
  (let [path (str (fs/cwd))
        do-all-pages (partial get-all-pages path)]
    (prn (pmap do-all-pages currencies)))
  (println "Etl finished..."))

(-main)
