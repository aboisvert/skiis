require 'buildr/scala'

VERSION_NUMBER = "0.1.0"

repositories.remote << "http://repo1.maven.org/maven2"

define "skiis" do
  project.version = VERSION_NUMBER
  project.group = "org.alexboisvert"
end
