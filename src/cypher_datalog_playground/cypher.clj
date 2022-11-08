(ns cypher-datalog-playground.cypher
  (:require [cypher-datalog-playground.northwind :as northwind])
  (:import (org.neo4j.driver GraphDatabase)
           (org.neo4j.driver AuthTokens)
           (org.neo4j.driver Result Session Transaction)))

(defn- to-map-without-keywords
  [map]
  (into {}
        (for [[k v] map]
          [(name k) v])))

(def import-queries
  [{:type  :category
    :query "WITH $row AS row MERGE (n:Category {categoryID: row.categoryID}) SET n = row"}
   {:type :supplier :query "WITH $row AS row MERGE (n:Supplier {supplierID: row.supplierID}) SET n = row"}
   {:type :product :query "WITH $row AS row MERGE (n:Product {productID: row.productID}) SET n = row, n.unitPrice = toFloat(row.unitPrice), n.unitsInStock = toInteger(row.unitsInStock), n.unitsOnOrder = toInteger(row.unitsOnOrder), n.reorderLevel = toInteger(row.reorderLevel), n.discontinued = (row.discontinued <> '0')"}
   {:type :customer :query "WITH $row AS row MERGE (n:Customer {customerID: row.customerID}) SET n = row"}
   {:type :order :query "WITH $row AS row MERGE (n:Order {orderID: row.orderID}) SET n = row"}
   {:type :employee :query "WITH $row AS row MERGE (e:Employee {employeeID: row.employeeID}) ON CREATE SET e.firstName = row.firstName, e.lastName = row.lastName, e.title = row.title"}
   {:type :employee :query "WITH $row AS row MATCH (employee:Employee {employeeID: row.employeeID}) MATCH (manager:Employee {employeeID: row.reportsTo}) MERGE (employee)-[:REPORTS_TO]->(manager)"}
   {:type :order-detail :query "WITH $row AS row MATCH (p:Product), (o:Order) WHERE p.productID = row.productID AND o.orderID = row.orderID MERGE (o)-[details:ORDERS]->(p) SET details = row, details.quantity = toInteger(row.quantity)"}
   {:query "MATCH (p:Product), (c:Category) WHERE p.categoryID = c.categoryID MERGE (p)-[:PART_OF]->(c)"}
   {:query "MATCH (p:Product), (s:Supplier) WHERE p.supplierID = s.supplierID MERGE (s)-[:SUPPLIES]->(p)"}
   {:query "MATCH (c:Customer), (o:Order)  WHERE c.customerID = o.customerID MERGE (c)-[:PURCHASED]->(o)"}
   {:query "MATCH (o:Order)  MATCH (e:Employee) WHERE o.employeeID = e.employeeID MERGE (e)-[:SOLD]->(o)"}])

(defn- run-import-query
  [tx query-description]
  (let [query (:query query-description)
        rows (map #(assoc {} "row" (to-map-without-keywords %)) (northwind/read-northwind-data (:type query-description)))]
    (println (str "Importing data for " (:type query-description)))
    (dorun (map #(.run tx query %) rows))))

(defn- import-data-to-neo4j
  [driver import-queries]
  (with-open [^Session session (.session driver)]
    (let [^Transaction tx (.beginTransaction session)]
      (dorun (map #(run-import-query tx %) (filter #(some? (:type %)) import-queries)))
      (dorun (map #(.run tx (:query %)) (filter #(nil? (:type %)) import-queries)))
      (.commit tx))))


(def queries
  [{:title "Get category of the product with name 'Chocolade'"
    :query "MATCH (:Product {productName:'Chocolade'})-[:PART_OF]->(category:Category)
    RETURN category.categoryName AS categoryName"}

   {:title "Get suppliers categories"
    :query "MATCH (supplier:Supplier)-->(:Product)-->(category:Category)
    RETURN supplier.companyName as company, collect(distinct category.categoryName) as categories
    ORDER BY company ASC LIMIT 10"}

   {:title "Find the suppliers of product in the Produce category"
    :query "MATCH (:Category {categoryName:\"Produce\"})<--(:Product)<--(supplier:Supplier)
    RETURN DISTINCT supplier.companyName as produceSuppliers"}

   {:title "Find a sample of employees who sold orders with their ordered products"
    :query "MATCH (employee:Employee)-[sold:SOLD]->(order:Order)-[orders:ORDERS]->(product:Product)
    RETURN employee, sold, order, orders, product LIMIT 25"}

   {:title "Which Employee had the Highest Cross-Selling Count of 'Raclette Courdavault' and Another Product?"
    :query "MATCH (choc:Product {productName:'Raclette Courdavault'})<-[:ORDERS]-(order:Order)-[:ORDERS]->(otherProduct:Product),
    (employee)-[:SOLD]->(order)
    RETURN employee.employeeID as employee, otherProduct.productName as otherProduct, count(otherProduct) as productCount
     ORDER BY productCount desc LIMIT 10"}

   {:title "How are Employees Organized? Who Reports to Whom?"
    :query "MATCH (employee:Employee)<-[:REPORTS_TO]-(subEmployee)
    RETURN employee.firstName + ' ' + employee.lastName AS manager, subEmployee.firstName + ' ' + subEmployee.lastName AS employee"}])


(defn- run-query-and-print-results
  [driver query-description]
  (with-open [^Session session (.session driver)]
    (let [^Transaction tx (.beginTransaction session)
          ^Result result (.run tx (:query query-description) (:parameters query-description {}))
          results (map #(.toString (.asMap %)) (.list result))]

      (do (println (str "\n" "Executed: " (:title query-description)))
          (println (str "Query: " (:query query-description)))
          (doseq [result results] (println result))
          (.commit tx)))))

(defn run-queries
  [neo4j-bolt-url auth]
  (let [driver (GraphDatabase/driver neo4j-bolt-url (AuthTokens/basic (:username auth) (:password auth)))]
    (do (println "Importing data to Neo4j database.")
        (import-data-to-neo4j driver import-queries)
        (println "Running queries against Neo4j database.")
        (doall (map #(run-query-and-print-results driver %) queries))
        (.close driver))))
