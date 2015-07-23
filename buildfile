require 'buildr/scala'

VERSION_NUMBER = "2.0.1"

repositories.remote << "http://repo1.maven.org/maven2"

define "skiis2_#{Buildr::Scala.version_without_build}" do
  project.version = VERSION_NUMBER
  project.group = "org.alexboisvert"
end
