"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[398],{3905:function(e,t,r){r.d(t,{Zo:function(){return s},kt:function(){return d}});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var u=n.createContext({}),c=function(e){var t=n.useContext(u),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},s=function(e){var t=c(e.components);return n.createElement(u.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},f=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,u=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),f=c(r),d=a,m=f["".concat(u,".").concat(d)]||f[d]||p[d]||i;return r?n.createElement(m,o(o({ref:t},s),{},{components:r})):n.createElement(m,o({ref:t},s))}));function d(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=f;var l={};for(var u in t)hasOwnProperty.call(t,u)&&(l[u]=t[u]);l.originalType=e,l.mdxType="string"==typeof e?e:a,o[1]=l;for(var c=2;c<i;c++)o[c]=r[c];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}f.displayName="MDXCreateElement"},5078:function(e,t,r){r.r(t),r.d(t,{assets:function(){return s},contentTitle:function(){return u},default:function(){return d},frontMatter:function(){return l},metadata:function(){return c},toc:function(){return p}});var n=r(7462),a=r(3366),i=(r(7294),r(3905)),o=["components"],l={sidebar_position:3},u="KVP table",c={unversionedId:"Streaming/kvp-table",id:"Streaming/kvp-table",title:"KVP table",description:"With a int serializer:",source:"@site/../zio-pravega-docs/target/mdoc/Streaming/kvp-table.md",sourceDirName:"Streaming",slug:"/Streaming/kvp-table",permalink:"/zio-pravega/docs/Streaming/kvp-table",draft:!1,tags:[],version:"current",sidebarPosition:3,frontMatter:{sidebar_position:3},sidebar:"tutorialSidebar",previous:{title:"Stream",permalink:"/zio-pravega/docs/Streaming/stream"}},s={},p=[{value:"Producer",id:"producer",level:2},{value:"Consumer",id:"consumer",level:2}],f={toc:p};function d(e){var t=e.components,r=(0,a.Z)(e,o);return(0,i.kt)("wrapper",(0,n.Z)({},f,r,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"kvp-table"},"KVP table"),(0,i.kt)("p",null,"With a int serializer:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"}," val intSerializer = new Serializer[Int] {\n    override def serialize(value: Int): ByteBuffer = {\n      val buff = ByteBuffer.allocate(4).putInt(value)\n      buff.position(0)\n      buff\n    }\n\n    override def deserialize(serializedValue: ByteBuffer): Int =\n      serializedValue.getInt\n  }\n\n  val tableWriterSettings = TableWriterSettingsBuilder(\n    new UTF8StringSerializer,\n    intSerializer\n  )\n    .build()\n\n  val tableReaderSettings = TableReaderSettingsBuilder(\n    new UTF8StringSerializer,\n    intSerializer\n  )\n    .build()\n")),(0,i.kt)("h2",{id:"producer"},"Producer"),(0,i.kt)("p",null,"To write in a stream."),(0,i.kt)("p",null,"Given a Key-Value ZStream:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'private def testStream(a: Int, b: Int): ZStream[Any, Nothing, (String, Int)] =\n    ZStream.fromIterable(a until b).map(i => (f"$i%04d", i))\n')),(0,i.kt)("p",null,"Just allocate a (K, V) sink ... et voil\xe0."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'def writeToTable: ZIO[PravegaTableService, Throwable, Boolean] =\n      ZIO.scoped(for {\n        sink <- PravegaTableService\n          .sink("tableName", tableWriterSettings, (a: Int, b: Int) => a + b)\n\n        _ <- testStream(0, 1000)\n          .run(sink)\n\n      } yield true)\n')),(0,i.kt)("h2",{id:"consumer"},"Consumer"))}d.isMDXComponent=!0}}]);