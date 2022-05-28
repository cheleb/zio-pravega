"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[5849],{3905:function(e,r,t){t.d(r,{Zo:function(){return l},kt:function(){return d}});var n=t(7294);function a(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function o(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);r&&(n=n.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,n)}return t}function i(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?o(Object(t),!0).forEach((function(r){a(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function c(e,r){if(null==e)return{};var t,n,a=function(e,r){if(null==e)return{};var t,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)t=o[n],r.indexOf(t)>=0||(a[t]=e[t]);return a}(e,r);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)t=o[n],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(a[t]=e[t])}return a}var p=n.createContext({}),s=function(e){var r=n.useContext(p),t=r;return e&&(t="function"==typeof e?e(r):i(i({},r),e)),t},l=function(e){var r=s(e.components);return n.createElement(p.Provider,{value:r},e.children)},u={inlineCode:"code",wrapper:function(e){var r=e.children;return n.createElement(n.Fragment,{},r)}},m=n.forwardRef((function(e,r){var t=e.components,a=e.mdxType,o=e.originalType,p=e.parentName,l=c(e,["components","mdxType","originalType","parentName"]),m=s(t),d=a,f=m["".concat(p,".").concat(d)]||m[d]||u[d]||o;return t?n.createElement(f,i(i({ref:r},l),{},{components:t})):n.createElement(f,i({ref:r},l))}));function d(e,r){var t=arguments,a=r&&r.mdxType;if("string"==typeof e||a){var o=t.length,i=new Array(o);i[0]=m;var c={};for(var p in r)hasOwnProperty.call(r,p)&&(c[p]=r[p]);c.originalType=e,c.mdxType="string"==typeof e?e:a,i[1]=c;for(var s=2;s<o;s++)i[s]=t[s];return n.createElement.apply(null,i)}return n.createElement.apply(null,t)}m.displayName="MDXCreateElement"},2720:function(e,r,t){t.r(r),t.d(r,{assets:function(){return l},contentTitle:function(){return p},default:function(){return d},frontMatter:function(){return c},metadata:function(){return s},toc:function(){return u}});var n=t(7462),a=t(3366),o=(t(7294),t(3905)),i=["components"],c={sidebar_position:1},p="Stream",s={unversionedId:"admin/stream",id:"admin/stream",title:"Stream",description:"Central Pravega abstraction, must be explictly created, in a Scope.",source:"@site/../zio-pravega-docs/target/mdoc/admin/stream.md",sourceDirName:"admin",slug:"/admin/stream",permalink:"/zio-pravega/docs/admin/stream",draft:!1,tags:[],version:"current",sidebarPosition:1,frontMatter:{sidebar_position:1},sidebar:"tutorialSidebar",previous:{title:"Scopes",permalink:"/zio-pravega/docs/admin/scope"},next:{title:"KVP Table",permalink:"/zio-pravega/docs/admin/kvp-table"}},l={},u=[],m={toc:u};function d(e){var r=e.components,t=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,n.Z)({},m,t,{components:r,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"stream"},"Stream"),(0,o.kt)("p",null,"Central Pravega abstraction, must be explictly created, in a ",(0,o.kt)("a",{parentName:"p",href:"/zio-pravega/docs/admin/scope"},"Scope"),"."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-scala"},'def initStream(streamName: String, scope: String)\n: ZIO[Scope & Console & PravegaAdminService,Throwable,Unit] =\n    for {\n      streamCreated <- PravegaAdminService.createStream(\n          scope,\n          streamName,\n          StreamConfiguration.builder\n            .scalingPolicy(ScalingPolicy.fixed(8))\n            .build\n        )\n      \n      _ <- ZIO.when(streamCreated)(\n        printLine(s"Stream $streamName just created")\n      )\n    } yield ()\n\n')),(0,o.kt)("h1",{id:"reader-group"},"Reader group"),(0,o.kt)("p",null,"A ",(0,o.kt)("a",{parentName:"p",href:"https://cncf.pravega.io/docs/nightly/pravega-concepts/#writers-readers-reader-groups"},"Reader Group")," is a named collection of Readers, which together perform parallel reads from a given Stream"),(0,o.kt)("p",null,"It must created expliciyly "),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-scala"},'  PravegaAdminService.readerGroup(\n              "a-scope",\n              "a-group-name",\n              "stream-a", "stream-b"\n            )\n')))}d.isMDXComponent=!0}}]);