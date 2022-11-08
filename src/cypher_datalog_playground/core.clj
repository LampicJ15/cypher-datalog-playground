(ns cypher-datalog-playground.core
  (:require [cypher-datalog-playground.cypher :as cypher]
            [cypher-datalog-playground.datalog :as datalog]))

(defn -main
  []
  (datalog/run-queries)
  (cypher/run-queries "bolt://localhost:7687" {:username "neo4j" :password "s3cr3t"})
  (System/exit 0))



