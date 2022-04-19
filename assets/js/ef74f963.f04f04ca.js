"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[825],{3905:function(e,r,t){t.d(r,{Zo:function(){return u},kt:function(){return g}});var n=t(7294);function a(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function i(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);r&&(n=n.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,n)}return t}function o(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?i(Object(t),!0).forEach((function(r){a(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):i(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function l(e,r){if(null==e)return{};var t,n,a=function(e,r){if(null==e)return{};var t,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)t=i[n],r.indexOf(t)>=0||(a[t]=e[t]);return a}(e,r);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)t=i[n],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(a[t]=e[t])}return a}var c=n.createContext({}),s=function(e){var r=n.useContext(c),t=r;return e&&(t="function"==typeof e?e(r):o(o({},r),e)),t},u=function(e){var r=s(e.components);return n.createElement(c.Provider,{value:r},e.children)},m={inlineCode:"code",wrapper:function(e){var r=e.children;return n.createElement(n.Fragment,{},r)}},p=n.forwardRef((function(e,r){var t=e.components,a=e.mdxType,i=e.originalType,c=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),p=s(t),g=a,d=p["".concat(c,".").concat(g)]||p[g]||m[g]||i;return t?n.createElement(d,o(o({ref:r},u),{},{components:t})):n.createElement(d,o({ref:r},u))}));function g(e,r){var t=arguments,a=r&&r.mdxType;if("string"==typeof e||a){var i=t.length,o=new Array(i);o[0]=p;var l={};for(var c in r)hasOwnProperty.call(r,c)&&(l[c]=r[c]);l.originalType=e,l.mdxType="string"==typeof e?e:a,o[1]=l;for(var s=2;s<i;s++)o[s]=t[s];return n.createElement.apply(null,o)}return n.createElement.apply(null,t)}p.displayName="MDXCreateElement"},6603:function(e,r,t){t.r(r),t.d(r,{contentTitle:function(){return c},default:function(){return p},frontMatter:function(){return l},metadata:function(){return s},toc:function(){return u}});var n=t(7462),a=t(3366),i=(t(7294),t(3905)),o=["components"],l={},c="Stream",s={unversionedId:"client/stream",id:"client/stream",title:"Stream",description:"Producer",source:"@site/../zio-pravega-docs/target/mdoc/client/stream.md",sourceDirName:"client",slug:"/client/stream",permalink:"/zio-pravega/docs/client/stream",tags:[],version:"current",frontMatter:{},sidebar:"tutorialSidebar",previous:{title:"Client",permalink:"/zio-pravega/docs/client/configuration"}},u=[{value:"Producer",id:"producer",children:[],level:2},{value:"Consumer",id:"consumer",children:[{value:"Group name",id:"group-name",children:[],level:3},{value:"Stream reader",id:"stream-reader",children:[],level:3}],level:2}],m={toc:u};function p(e){var r=e.components,t=(0,a.Z)(e,o);return(0,i.kt)("wrapper",(0,n.Z)({},m,t,{components:r,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"stream"},"Stream"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},"import zio.Console._\nimport zio.stream._\nimport zio.pravega._\nimport io.pravega.client.stream.impl.UTF8StringSerializer\n")),(0,i.kt)("h2",{id:"producer"},"Producer"),(0,i.kt)("p",null,"To write in a stream, simply bread a Sink:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'val writterSettings =\n    WriterSettingsBuilder()\n      .withSerializer(new UTF8StringSerializer)\n// writterSettings: WriterSettings[String] = zio.pravega.WriterSettings@34de8c9f\nval sink = PravegaStream(_.sink("my-stream", writterSettings))\n// sink: zio.ZIO[PravegaStreamService with zio.Scope, Throwable, ZSink[Any, Throwable, String, Nothing, Unit]] = zio.ZIO$Suspend@137a70f1\n')),(0,i.kt)("h2",{id:"consumer"},"Consumer"),(0,i.kt)("h3",{id:"group-name"},"Group name"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'val group = PravegaAdmin(\n        _.readerGroup(\n          "my-scope",\n          "my-group",\n          "my-stream"\n        )\n      )\n// group: zio.ZIO[PravegaAdminService, Throwable, Boolean] = zio.ZIO$Suspend@5f1bd673\n')),(0,i.kt)("h3",{id:"stream-reader"},"Stream reader"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'val readerSettings =\n    ReaderSettingsBuilder()\n      .withSerializer(new UTF8StringSerializer)\n// readerSettings: ReaderSettings[String] = zio.pravega.ReaderSettings@54351197\nval stream = PravegaStream(_.stream("mygroup", readerSettings))\n// stream: zio.ZIO[PravegaStreamService, Throwable, ZStream[Any, Throwable, String]] = zio.ZIO$Suspend@144fc5fa\n')),(0,i.kt)("h1",{id:"all-together"},"All together"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'def testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =\n    Stream.fromIterable(a until b).map(i => s"ZIO Message $i")\n\nval n = 10\n// n: Int = 10\n\nfor {\n      sink <- PravegaStream(_.sink("my-stream", writterSettings))\n      _ <- testStream(0, 10).run(sink)\n      _ <- group\n      stream <- PravegaStream(_.stream("my-group", readerSettings))\n      count <- stream\n        .take(n.toLong * 2)\n        .tap(e => printLine(s"ZStream of [$e]"))\n        .runFold(0)((s, _) => s + 1)\n    } yield count\n// res0: zio.ZIO[PravegaStreamService with zio.Scope with PravegaAdminService with PravegaStreamService, Throwable, Int] = <function1>\n')))}p.isMDXComponent=!0}}]);