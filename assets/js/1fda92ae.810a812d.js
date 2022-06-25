"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[2754],{3905:(e,r,t)=>{t.d(r,{Zo:()=>l,kt:()=>d});var n=t(7294);function o(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function a(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);r&&(n=n.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,n)}return t}function i(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?a(Object(t),!0).forEach((function(r){o(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):a(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function c(e,r){if(null==e)return{};var t,n,o=function(e,r){if(null==e)return{};var t,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)t=a[n],r.indexOf(t)>=0||(o[t]=e[t]);return o}(e,r);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)t=a[n],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(o[t]=e[t])}return o}var p=n.createContext({}),s=function(e){var r=n.useContext(p),t=r;return e&&(t="function"==typeof e?e(r):i(i({},r),e)),t},l=function(e){var r=s(e.components);return n.createElement(p.Provider,{value:r},e.children)},u={inlineCode:"code",wrapper:function(e){var r=e.children;return n.createElement(n.Fragment,{},r)}},m=n.forwardRef((function(e,r){var t=e.components,o=e.mdxType,a=e.originalType,p=e.parentName,l=c(e,["components","mdxType","originalType","parentName"]),m=s(t),d=o,f=m["".concat(p,".").concat(d)]||m[d]||u[d]||a;return t?n.createElement(f,i(i({ref:r},l),{},{components:t})):n.createElement(f,i({ref:r},l))}));function d(e,r){var t=arguments,o=r&&r.mdxType;if("string"==typeof e||o){var a=t.length,i=new Array(a);i[0]=m;var c={};for(var p in r)hasOwnProperty.call(r,p)&&(c[p]=r[p]);c.originalType=e,c.mdxType="string"==typeof e?e:o,i[1]=c;for(var s=2;s<a;s++)i[s]=t[s];return n.createElement.apply(null,i)}return n.createElement.apply(null,t)}m.displayName="MDXCreateElement"},2803:(e,r,t)=>{t.r(r),t.d(r,{assets:()=>p,contentTitle:()=>i,default:()=>u,frontMatter:()=>a,metadata:()=>c,toc:()=>s});var n=t(7462),o=(t(7294),t(3905));const a={sidebar_position:1},i="Scopes",c={unversionedId:"admin/scope",id:"admin/scope",title:"Scopes",description:"Scopes are namespaces for Streams, they must be created before being referenced (no automatic creation).",source:"@site/../zio-pravega-docs/target/mdoc/admin/scope.md",sourceDirName:"admin",slug:"/admin/scope",permalink:"/zio-pravega/docs/admin/scope",draft:!1,tags:[],version:"current",sidebarPosition:1,frontMatter:{sidebar_position:1},sidebar:"tutorialSidebar",previous:{title:"Serializer",permalink:"/zio-pravega/docs/Serializer"},next:{title:"Stream",permalink:"/zio-pravega/docs/admin/stream"}},p={},s=[],l={toc:s};function u(e){let{components:r,...t}=e;return(0,o.kt)("wrapper",(0,n.Z)({},l,t,{components:r,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"scopes"},"Scopes"),(0,o.kt)("p",null,"Scopes are namespaces for ",(0,o.kt)("a",{parentName:"p",href:"/zio-pravega/docs/admin/stream"},"Streams"),", they must be created before being referenced (no automatic creation)."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-scala"},'\ndef initScope(scope: String): ZIO[Scope & PravegaAdminService & Console,Throwable,Unit] =\n    for {\n      scopeCreated <- PravegaAdmin.createScope(scope)\n      _ <- ZIO.when(scopeCreated)(printLine(s"Scope $scope just created"))\n    } yield ()\n\n')))}u.isMDXComponent=!0}}]);