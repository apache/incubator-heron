To get IntelliJ IDEA to use the Heron conventions for Java checkstyle (which should roughly
correspond to the build checkstyle):

1. Find the location of your IntelliJ settings directory:
    * `~/Library/Preferences/IntelliJIdea14/` on MacOSX
    * `~/.IntelliJIdea14/` on Linux
2. Symlink `tools/java/src/com/twitter/bazel/checkstyle/HeronIDEA.xml` from the Heron repo
   to `<settings_dir>/codestyles/HeronIDEA.xml`
3. Restart IntelliJ.
4. `IntelliJ IDEA -> Preferences -> Editor -> Code Style -> Java -> Scheme` and select "Heron" from the pulldown menu.

Now doing a Code -> Optimize Imports in IntelliJ will (mostly) follow Heron style.
