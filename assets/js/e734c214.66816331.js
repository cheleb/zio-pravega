"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[8806],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>m});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var c=r.createContext({}),s=function(e){var t=r.useContext(c),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},u=function(e){var t=s(e.components);return r.createElement(c.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},f=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,c=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),f=s(n),m=o,d=f["".concat(c,".").concat(m)]||f[m]||p[m]||a;return n?r.createElement(d,i(i({ref:t},u),{},{components:n})):r.createElement(d,i({ref:t},u))}));function m(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,i=new Array(a);i[0]=f;var l={};for(var c in t)hasOwnProperty.call(t,c)&&(l[c]=t[c]);l.originalType=e,l.mdxType="string"==typeof e?e:o,i[1]=l;for(var s=2;s<a;s++)i[s]=n[s];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}f.displayName="MDXCreateElement"},5907:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>c,contentTitle:()=>i,default:()=>p,frontMatter:()=>a,metadata:()=>l,toc:()=>s});var r=n(7462),o=(n(7294),n(3905));const a={sidebar_position:3},i="Client configuration",l={unversionedId:"client",id:"client",title:"Client configuration",description:"Pravega client configuration has many options. Default values are found in reference.conf and can be overriden globaly in application.conf or locally in a programatic way.",source:"@site/../zio-pravega-docs/target/mdoc/client.md",sourceDirName:".",slug:"/client",permalink:"/zio-pravega/docs/client",draft:!1,tags:[],version:"current",sidebarPosition:3,frontMatter:{sidebar_position:3},sidebar:"tutorialSidebar",previous:{title:"Read from stream",permalink:"/zio-pravega/docs/quickstart/read"},next:{title:"Serializer",permalink:"/zio-pravega/docs/Serializer"}},c={},s=[],u={toc:s};function p(e){let{components:t,...n}=e;return(0,o.kt)("wrapper",(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"client-configuration"},"Client configuration"),(0,o.kt)("p",null,"Pravega client configuration has many options. Default values are found in ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/cheleb/zio-pravega/blob/master/src/main/resources/reference.conf"},"reference.conf")," and can be overriden globaly in application.conf or locally in a programatic way."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-scala"},'val clientConfig = ClientConfig\n    .builder()\n    .controllerURI(new URI("tcp://localhost:9090:"))\n    .enableTlsToController(true)\n    .build()\n// clientConfig: ClientConfig = ClientConfig(controllerURI=tcp://localhost:9090:, credentials=null, trustStore=null, validateHostName=true, maxConnectionsPerSegmentStore=10, isDefaultMaxConnections=true, deriveTlsEnabledFromControllerURI=false, enableTlsToController=true, enableTlsToSegmentStore=false, metricListener=null)\n')))}p.isMDXComponent=!0}}]);