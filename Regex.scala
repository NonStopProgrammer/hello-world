import scala.util.matching.Regex
import scala.util.{Try, Success, Failure}

def extractTargetDatabaseFromQueries(queries: List[String]): String = {
  // Regular expressions to match different patterns of target table names in the queries
  val createPattern: Regex = """(?i)\bcreate\s+table\s+(\[?\w+\]?)\.(\[?\w+\]?)\.(\[?\w+\]?)""".r
  val deletePattern: Regex = """(?i)\bdelete\s+from\s+(\[?\w+\]?)\.(\[?\w+\]?)\.(\[?\w+\]?)""".r
  val dropPattern: Regex = """(?i)\bdrop\s+table\s+(if\s+exists\s+)?(\[?\w+\]?)\.(\[?\w+\]?)\.(\[?\w+\]?)""".r
  val truncatePattern: Regex = """(?i)\btruncate\s+table\s+(\[?\w+\]?)\.(\[?\w+\]?)\.(\[?\w+\]?)""".r
  val mergePattern: Regex = """(?i)\bmerge\s+into\s+(\[?\w+\]?)\.(\[?\w+\]?)\.(\[?\w+\]?)""".r
  val updatePattern: Regex = """(?i)\bupdate\s+(\[?\w+\]?)\.(\[?\w+\]?)\.(\[?\w+\]?)""".r
  val insertPattern: Regex = """(?i)\binsert\s+into\s+(\[?\w+\]?)\.(\[?\w+\]?)\.(\[?\w+\]?)""".r
  val selectIntoPattern: Regex = """(?i)\bselect\b.+?\binto\s+(\[?\w+\]?)\.(\[?\w+\]?)\.(\[?\w+\]?)""".r

  // Initialize an empty list to collect target table names with preference order
  var tables = List[String]()

  // Helper function to extract database name from full table name
  def extractDatabaseName(tableName: String): String = {
    val parts = tableName.split("\\.")
    if (parts.length > 0) parts(0).replace("[", "").replace("]", "") else ""
  }

  // Process each query in the input list
  queries.foreach { query =>
    val result = Try {
      // Extract target tables from the query and add to the list with preference
      selectIntoPattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
      insertPattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
      if (tables.isEmpty) {
        createPattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
        deletePattern.findAllMatchIn(query).foreach(m => tables ::= m.group(1))
        dropPattern.findAllMatchIn(query).foreach(m => tables ::= m.group(2))
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

val targetDatabase = extractTargetDatabaseFromQueries(queries)
println(targetDatabase) // Output should be dbRawHogan
