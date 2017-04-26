import AssemblyKeys._

// put this at the top of the file

assemblySettings

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)