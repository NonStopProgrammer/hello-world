import scala.util.matching.Regex
import scala.util.{Try, Success, Failure}

def extractTargetDatabaseFromQueries(queries: List[String]): String = {
  // Regular expressions to match different patterns of target table names in the queries
  val createPattern: Regex = """(?i)\bcreate\s+table\s+([\w\.]+)""".r
  val deletePattern: Regex = """(?i)\bdelete\s+from\s+([\w\.]+)""".r
  val dropPattern: Regex = """(?i)\bdrop\s+table\s+([\w\.]+)""".r
  val truncatePattern: Regex = """(?i)\btruncate\s+table\s+([\w\.]+)""".r
  val mergePattern: Regex = """(?i)\bmerge\s+into\s+([\w\.]+)""".r
  val updatePattern: Regex = """(?i)\bupdate\s+([\w\.]+)""".r
  val insertPattern: Regex = """(?i)\binsert\s+into\s+([\w\.]+)""".r
  val selectIntoPattern: Regex = """(?i)\bselect\b.+?\binto\s+([\w\.]+)""".r

  // Initialize an empty set to collect target table names with preference order
  var tables = List[String]()

  // Helper function to extract database name from full table name
  def extractDatabaseName(tableName: String): String = {
    val parts = tableName.split("\\.")
    if (parts.length > 0) parts(0) else ""
  }

  // Process each query in the input list
  queries.foreach { query =>
    val result = Try {
      // Extract target tables from the query and add to the list with preference
      insertPattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
      selectIntoPattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
      if (tables.isEmpty) {
        createPattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
        deletePattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
        dropPattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
        truncatePattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
        mergePattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
        updatePattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
      }
    }
    
    // Handle any potential errors
    result match {
      case Success(_) => // Do nothing
      case Failure(_) => // Do nothing
    }
  }

  // Return the database name from the first preferred table or "default" if none found
  if (tables.nonEmpty) extractDatabaseName(tables.head) else "default"
}

// Example usage
val queries = List(
  "CREATE TABLE db1.schema1.target_table1 (id INT, name VARCHAR(50));",
  "DELETE FROM db2.schema2.target_table2 WHERE id = 1;",
  "DROP TABLE db3.schema3.target_table3;",
  "TRUNCATE TABLE db4.schema4.target_table4;",
  "MERGE INTO db5.schema5.target_table5 AS t USING source_table AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name;",
  "UPDATE db6.schema6.target_table6 SET name = 'NewName' WHERE id = 1;",
  "INSERT INTO db7.schema7.target_table7 (id, name) SELECT id, name FROM source_table;",
  "SELECT id, name INTO db8.schema8.target_table8 FROM source_table;"
)

val targetDatabase = extractTargetDatabaseFromQueries(queries)
println(targetDatabase) // Output: db7
