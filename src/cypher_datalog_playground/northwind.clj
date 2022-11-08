(ns cypher-datalog-playground.northwind
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(def northwind-data {:category "resources/northwind/categories.csv"
                     :customer "resources/northwind/customers.csv"
                     :employee "resources/northwind/employees.csv"
                     :order-detail "resources/northwind/order-details.csv"
                     :order "resources/northwind/orders.csv"
                     :product "resources/northwind/products.csv"
                     :supplier "resources/northwind/suppliers.csv"})

(defn- csv-data->maps
  [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map keyword)
            repeat)
       (rest csv-data)))

(defn read-northwind-data
  [data-type]
  (csv-data->maps (with-open [csv-reader (io/reader (northwind-data data-type))]
                    (doall (csv/read-csv csv-reader)))))