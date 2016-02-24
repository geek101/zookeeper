In order to build maven artifacts, first update the version in the build.xml file, e.g.:

    <property name="version" value="3.4.8-square-ssl-patch-5" />

And then build it with ant:

% ant clean package

This will compile everything and build maven artifacts, which will be placed in a versioned
directory, e.g.:

./build/zookeeper-3.4.8-square-ssl-patch-5/dist-maven/zookeeper-3.4.8-square-ssl-patch-5-javadoc.jar
./build/zookeeper-3.4.8-square-ssl-patch-5/dist-maven/zookeeper-3.4.8-square-ssl-patch-5-sources.jar
./build/zookeeper-3.4.8-square-ssl-patch-5/dist-maven/zookeeper-3.4.8-square-ssl-patch-5.jar
./build/zookeeper-3.4.8-square-ssl-patch-5/dist-maven/zookeeper-3.4.8-square-ssl-patch-5.pom

These can then be uploaded to our nexus repo.

********************************************************************************

Release version info:

3.4.8-square-ssl-patch-5:
  - this version is rebased off the released 3.4.8 source
  - patch-5 fixes a packaging issue so the correct version is displayed correctly, on startup
