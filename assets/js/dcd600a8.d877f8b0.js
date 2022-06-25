"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[6832],{3905:(e,t,r)=>{r.d(t,{Zo:()=>c,kt:()=>g});var a=r(7294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},i=Object.keys(e);for(a=0;a<i.length;a++)r=i[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)r=i[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var l=a.createContext({}),m=function(e){var t=a.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},c=function(e){var t=m(e.components);return a.createElement(l.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},u=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,i=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),u=m(r),g=n,d=u["".concat(l,".").concat(g)]||u[g]||p[g]||i;return r?a.createElement(d,o(o({ref:t},c),{},{components:r})):a.createElement(d,o({ref:t},c))}));function g(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=r.length,o=new Array(i);o[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:n,o[1]=s;for(var m=2;m<i;m++)o[m]=r[m];return a.createElement.apply(null,o)}return a.createElement.apply(null,r)}u.displayName="MDXCreateElement"},946:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>p,frontMatter:()=>i,metadata:()=>s,toc:()=>m});var a=r(7462),n=(r(7294),r(3905));const i={sidebar_position:2},o="Stream",s={unversionedId:"Streaming/stream",id:"Streaming/stream",title:"Stream",description:"Stream writer",source:"@site/../zio-pravega-docs/target/mdoc/Streaming/stream.md",sourceDirName:"Streaming",slug:"/Streaming/stream",permalink:"/zio-pravega/docs/Streaming/stream",draft:!1,tags:[],version:"current",sidebarPosition:2,frontMatter:{sidebar_position:2},sidebar:"tutorialSidebar",previous:{title:"Settings",permalink:"/zio-pravega/docs/Streaming/settings"},next:{title:"Settings",permalink:"/zio-pravega/docs/KVPTable/settings"}},l={},m=[{value:"Stream writer",id:"stream-writer",level:2},{value:"Stream reader",id:"stream-reader",level:2}],c={toc:m};function p(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"stream"},"Stream"),(0,n.kt)("h2",{id:"stream-writer"},"Stream writer"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Without transaction in a stream, simply create a Sink:")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},'val sink = PravegaStream.sink("my-stream", writerSettings)\n')),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"With transaction, the transaction will commit at the end of the Stream.")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},'val sinkTx = PravegaStream.sinkTx("my-stream", writerSettings)\n')),(0,n.kt)("h2",{id:"stream-reader"},"Stream reader"),(0,n.kt)("p",null,"To read from a stream, simply create a stream:"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},'val stream = PravegaStream.stream("mygroup", readerSettings)\n')),(0,n.kt)("h1",{id:"all-together"},"All together"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},'// A Stream of strings\ndef testStream(a: Int, b: Int): ZStream[Any, Nothing, String] =\n    ZStream.fromIterable(a until b).map(i => s"ZIO Message $i")\n\nval n = 10\n\nfor {\n      sink <- PravegaStream.sink("my-stream", writerSettings)\n      _ <- testStream(0, 10).run(sink)\n      stream <- PravegaStream.stream("my-group", readerSettings)\n      count <- stream\n        .take(n.toLong * 2)\n        .tap(e => printLine(s"ZStream of [$e]"))\n        .runFold(0)((s, _) => s + 1)\n    } yield count\n')))}p.isMDXComponent=!0}}]);