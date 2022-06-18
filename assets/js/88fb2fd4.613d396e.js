"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[6835],{3905:function(e,r,t){t.d(r,{Zo:function(){return s},kt:function(){return m}});var n=t(7294);function a(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function o(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);r&&(n=n.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,n)}return t}function i(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?o(Object(t),!0).forEach((function(r){a(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function c(e,r){if(null==e)return{};var t,n,a=function(e,r){if(null==e)return{};var t,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)t=o[n],r.indexOf(t)>=0||(a[t]=e[t]);return a}(e,r);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)t=o[n],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(a[t]=e[t])}return a}var p=n.createContext({}),u=function(e){var r=n.useContext(p),t=r;return e&&(t="function"==typeof e?e(r):i(i({},r),e)),t},s=function(e){var r=u(e.components);return n.createElement(p.Provider,{value:r},e.children)},l={inlineCode:"code",wrapper:function(e){var r=e.children;return n.createElement(n.Fragment,{},r)}},f=n.forwardRef((function(e,r){var t=e.components,a=e.mdxType,o=e.originalType,p=e.parentName,s=c(e,["components","mdxType","originalType","parentName"]),f=u(t),m=a,d=f["".concat(p,".").concat(m)]||f[m]||l[m]||o;return t?n.createElement(d,i(i({ref:r},s),{},{components:t})):n.createElement(d,i({ref:r},s))}));function m(e,r){var t=arguments,a=r&&r.mdxType;if("string"==typeof e||a){var o=t.length,i=new Array(o);i[0]=f;var c={};for(var p in r)hasOwnProperty.call(r,p)&&(c[p]=r[p]);c.originalType=e,c.mdxType="string"==typeof e?e:a,i[1]=c;for(var u=2;u<o;u++)i[u]=t[u];return n.createElement.apply(null,i)}return n.createElement.apply(null,t)}f.displayName="MDXCreateElement"},2888:function(e,r,t){t.r(r),t.d(r,{assets:function(){return s},contentTitle:function(){return p},default:function(){return m},frontMatter:function(){return c},metadata:function(){return u},toc:function(){return l}});var n=t(7462),a=t(3366),o=(t(7294),t(3905)),i=["components"],c={sidebar_position:3},p="Read from stream",u={unversionedId:"quickstart/read",id:"quickstart/read",title:"Read from stream",description:"",source:"@site/../zio-pravega-docs/target/mdoc/quickstart/read.md",sourceDirName:"quickstart",slug:"/quickstart/read",permalink:"/zio-pravega/docs/quickstart/read",draft:!1,tags:[],version:"current",sidebarPosition:3,frontMatter:{sidebar_position:3},sidebar:"tutorialSidebar",previous:{title:"Write to stream",permalink:"/zio-pravega/docs/quickstart/write"},next:{title:"Client configuration",permalink:"/zio-pravega/docs/client"}},s={},l=[],f={toc:l};function m(e){var r=e.components,t=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,n.Z)({},f,t,{components:r,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"read-from-stream"},"Read from stream"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-scala"},'import zio._\nimport zio.pravega._\n\nimport io.pravega.client.stream.impl.UTF8StringSerializer\n\nobject StreamReadExample extends ZIOAppDefault {\n\n  val clientConfig = PravegaClientConfig.default\n\n  val stringReaderSettings =\n    ReaderSettingsBuilder()\n      .withSerializer(new UTF8StringSerializer)\n\n  private val program = for {\n    _ <- PravegaAdmin.readerGroup(\n      "a-scope",\n      "a-reader-group",\n      "a-stream"\n    )\n    stream <- PravegaStream.stream(\n      "a-reader-group",\n      stringReaderSettings\n    )\n    _ <- stream\n      .tap(m => ZIO.debug(m.toString()))\n      .take(10)\n      .runFold(0)((s, _) => s + 1)\n\n  } yield ()\n\n  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =\n    program.provide(\n      Scope.default,\n      PravegaAdmin.live(clientConfig),\n      PravegaStream.fromScope("a-scope", clientConfig)\n    )\n\n}\n')))}m.isMDXComponent=!0}}]);