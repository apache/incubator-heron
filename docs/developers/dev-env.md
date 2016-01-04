# Setting Up a Development Environment



#### Maven

<pre><code class="lang-xml">&lt;dependency&gt;
  &lt;groupId>com.twitter.heron&lt;/groupId&gt;
  &lt;artifactId>scheduler&lt;/artifactId&gt;
  &lt;version&gt;{{book.scheduler_api_version}}&lt;/version&gt;
&lt;/dependency&gt;</code></pre>

#### Gradle

<pre><code class="lang-groovy">dependencies {
  compile group: "com.twitter.heron", name: "scheduler", version: "{{book.scheduler_api_version}}"
}</code></pre>

