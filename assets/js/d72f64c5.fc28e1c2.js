"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[2459],{3905:(e,t,r)=>{r.d(t,{Zo:()=>s,kt:()=>b});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var p=n.createContext({}),c=function(e){var t=n.useContext(p),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},s=function(e){var t=c(e.components);return n.createElement(p.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,p=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),m=c(r),b=a,d=m["".concat(p,".").concat(b)]||m[b]||u[b]||o;return r?n.createElement(d,i(i({ref:t},s),{},{components:r})):n.createElement(d,i({ref:t},s))}));function b(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=m;var l={};for(var p in t)hasOwnProperty.call(t,p)&&(l[p]=t[p]);l.originalType=e,l.mdxType="string"==typeof e?e:a,i[1]=l;for(var c=2;c<o;c++)i[c]=r[c];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},8848:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>p,contentTitle:()=>i,default:()=>u,frontMatter:()=>o,metadata:()=>l,toc:()=>c});var n=r(7462),a=(r(7294),r(3905));const o={sidebar_position:3},i="KVP Table",l={unversionedId:"admin/kvp-table",id:"admin/kvp-table",title:"KVP Table",description:"Key Value Pair) tables are an early feature.",source:"@site/../zio-pravega-docs/target/mdoc/admin/kvp-table.md",sourceDirName:"admin",slug:"/admin/kvp-table",permalink:"/zio-pravega/docs/admin/kvp-table",draft:!1,tags:[],version:"current",sidebarPosition:3,frontMatter:{sidebar_position:3},sidebar:"tutorialSidebar",previous:{title:"Stream",permalink:"/zio-pravega/docs/admin/stream"},next:{title:"Settings",permalink:"/zio-pravega/docs/Streaming/settings"}},p={},c=[],s={toc:c};function u(e){let{components:t,...r}=e;return(0,a.kt)("wrapper",(0,n.Z)({},s,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"kvp-table"},"KVP Table"),(0,a.kt)("p",null,(0,a.kt)("a",{parentName:"p",href:"https://github.com/pravega/pravega/wiki/PDP-48-(Key-Value-Tables-Beta-2)"},"Key Value Pair")," tables are an early feature."),(0,a.kt)("p",null,"In the same way as ",(0,a.kt)("a",{parentName:"p",href:"/zio-pravega/docs/admin/stream"},"streams")," KVP table belong to a ",(0,a.kt)("a",{parentName:"p",href:"/zio-pravega/docs/admin/scope"},"scope"),". "),(0,a.kt)("p",null,"It must be explictly created"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-scala"},'\nval tableConfig = KeyValueTableConfiguration\n    .builder()\n    .partitionCount(2)\n    .primaryKeyLength(4)\n    .build()\n\ndef initTable(tableName: String, pravegaScope: String)\n: ZIO[Scope & Console & PravegaAdminService,Throwable,Unit] =\n    for {\n      tableCreated <- PravegaAdmin.createTable(\n              tableName,\n              tableConfig,\n              pravegaScope\n            )\n      \n      _ <- ZIO.when(tableCreated)(\n        printLine(s"Table $tableName just created")\n      )\n    } yield ()\n\n')))}u.isMDXComponent=!0}}]);