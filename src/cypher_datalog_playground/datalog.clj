(ns cypher-datalog-playground.datalog
  (:require [cypher-datalog-playground.northwind :as northwind]
            [xtdb.api :as xt])
  (:import (java.util UUID)))

(def xtdb (xt/start-node {}))

;; import northwind data to xtdb
(defn- put-to-xtdb
  [data-type]
  (do (println (str "Importing data for data type " data-type))
      (xt/await-tx xtdb (xt/submit-tx xtdb
                                      (map
                                        #(conj [::xt/put]
                                               (assoc % :xt/id (UUID/randomUUID)))
                                        (northwind/read-northwind-data data-type))))))

(doall (map #(put-to-xtdb %) (keys northwind/northwind-data)))

;; queries
(def queries
  [{:title "Get category of the product with name 'Chocolade'"
    :query '{:find  [?category-name]
             :keys  [category-name]
             :where [[product :productName "Chocolade"]
                     [product :categoryID categoryId]
                     [category :categoryID categoryId]
                     [category :categoryName ?category-name]]}}

   {:title "Get suppliers categories"
    :query '{:find     [?company (distinct ?category-name)]
             :keys     [supplier supplied-categories]
             :order-by [[?company :asc]]
             :where    [[product :supplierID supplier-id]
                        [product :categoryID category-id]
                        [company :supplierID supplier-id]
                        [company :companyName ?company]
                        [category :categoryID category-id]
                        [category :categoryName ?category-name]]}}

   {:title "Find the suppliers of product in the Produce category"
    :query '{:find  [(distinct ?produce-supplier)]
             :keys  [produce-suppliers]
             :where [[category :categoryName "Produce"]
                     [category :categoryID category-id]
                     [product :categoryID category-id]
                     [product :supplierID supplier-id]
                     [supplier :supplierID supplier-id]
                     [supplier :companyName ?produce-supplier]]}}


   {:title "Find a sample of employees who sold orders with their ordered products"
    :query '{:find  [(pull ?employee [*])
                     (pull ?order [*])
                     (pull ?product [*])]
             :keys  [employee order product]
             :limit 5
             :where [[?order :employeeID employee-id]
                     [?employee :employeeID employee-id]
                     [?order :orderID order-id]
                     [order-detail :orderID order-id]
                     [order-detail :productID product-id]
                     [?product :productID product-id]
                     [?product :productName _]]}}

   {:title "Which Employee had the Highest Cross-Selling Count of 'Raclette Courdavault' and Another Product?"
    :query '{:find     [?employee-id ?other-product (count other-order-detail)]
             :keys     [employee otherProduct otherProductCount]
             :order-by [[(count other-order-detail) :desc]]
             :limit    10
             :where    [[raclette-product :productName "Raclette Courdavault"]
                        [raclette-product :productID raclette-id]

                        [raclette-order-detail :productID raclette-id]
                        [raclette-order-detail :orderID raclette-order-id]

                        [order :orderID raclette-order-id]
                        [order :employeeID ?employee-id]

                        [other-order-detail :orderID raclette-order-id]
                        [(not= other-order-detail raclette-order-detail)]
                        [other-order-detail :productID other-product-id]
                        [other-product :productID other-product-id]
                        [other-product :productName ?other-product]]}}

   {:title "How are Employees Organized? Who Reports to Whom?"
    :query '{:find  [(str ?employee-name " " ?employee-surname) (str ?manager-name " " ?manager-surname)]
             :keys  [employee reports-to]
             :where [[manager :employeeID manager-id]
                     [employee :reportsTo manager-id]
                     [manager :firstName ?manager-name]
                     [manager :lastName ?manager-surname]
                     [employee :firstName ?employee-name]
                     [employee :lastName ?employee-surname]]}}])


(defn- run-query-and-print-results
  [query-description]
  (let [result (xt/q (xt/db xtdb) (:query query-description))]
    (do (println (str "\n" "Executed: " (:title query-description)))
        (println (str "Query: " (:query query-description) "\n"))
        (run! println result))))


(defn run-queries
  []
  (doall (map #(run-query-and-print-results %) queries)))

