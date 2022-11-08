(defproject cypher-datalog-playground "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :main cypher-datalog-playground.core
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.csv "1.0.1"]
                 [com.xtdb/xtdb-core "1.22.0"]
                 [org.neo4j.driver/neo4j-java-driver "4.4.9"]]
  :repl-options {:init-ns cypher-datalog-playground.core})
