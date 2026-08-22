var h0=Object.defineProperty,bT=Object.defineProperties;var CT=Object.getOwnPropertyDescriptors;var u0=Object.getOwnPropertySymbols;var wT=Object.prototype.hasOwnProperty,ST=Object.prototype.propertyIsEnumerable;var f0=(t,n,e)=>n in t?h0(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,C=(t,n)=>{for(var e in n||={})wT.call(n,e)&&f0(t,e,n[e]);if(u0)for(var e of u0(n))ST.call(n,e)&&f0(t,e,n[e]);return t},re=(t,n)=>bT(t,CT(n));var DT=(t,n)=>{for(var e in n)h0(t,e,{get:n[e],enumerable:!0})};var sn=null,tu=!1,Po=1,ET=null,_t=Symbol("SIGNAL");function ce(t){let n=sn;return sn=t,n}function nu(){return sn}var qr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Yr(t){if(tu)throw new Error("");if(sn===null)return;sn.consumerOnSignalRead(t);let n=sn.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=sn.recomputing;if(i&&(e=n!==void 0?n.nextProducer:sn.producers,e!==void 0&&e.producer===t)){sn.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=Po;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===sn&&(!i||r.knownValidAtEpoch===Po))return;let o=Qa(sn),a={producer:t,consumer:sn,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:Po,lastReadVersion:t.version,nextConsumer:void 0};sn.producersTail=a,n!==void 0?n.nextProducer=a:sn.producers=a,o&&_0(t,a)}function m0(){Po++}function Vo(t){if(!(Qa(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Po)){if(!t.producerMustRecompute(t)&&!Za(t)){Ka(t);return}t.producerRecomputeValue(t),Ka(t)}}function up(t){if(t.consumers===void 0)return;let n=tu;tu=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||xT(i)}}finally{tu=n}}function fp(){return sn?.consumerAllowSignalWrites!==!1}function xT(t){t.dirty=!0,up(t),t.consumerMarkedDirty?.(t)}function Ka(t){t.dirty=!1,t.lastCleanEpoch=Po}function hr(t){return t&&p0(t),ce(t)}function p0(t){if(t.producersTail?.knownValidAtEpoch===Po){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function Kr(t,n){ce(n),t&&g0(t)}function g0(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Qa(t))do e=hp(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Za(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Vo(e),i!==e.version))return!0}return!1}function Zr(t){if(Qa(t)){let n=t.producers;for(;n!==void 0;)n=hp(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function _0(t,n){let e=t.consumersTail,i=Qa(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)_0(r.producer,r)}function hp(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Qa(n)){let o=n.producers;for(;o!==void 0;)o=hp(o)}return e}function Qa(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Tc(t){ET?.(t)}function kc(t,n){return Object.is(t,n)}function Ac(t,n){let e=Object.create(MT);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Vo(e),Yr(e),e.value===Ui)throw e.error;return e.value};return i[_t]=e,Tc(e),i}var Fo=Symbol("UNSET"),Lo=Symbol("COMPUTING"),Ui=Symbol("ERRORED"),MT=re(C({},qr),{value:Fo,dirty:!0,error:null,equal:kc,kind:"computed",producerMustRecompute(t){return t.value===Fo||t.value===Lo},producerRecomputeValue(t){if(t.value===Lo)throw new Error("");let n=t.value;t.value=Lo;let e=hr(t),i,r=!1;try{i=t.computation(),ce(null),r=n!==Fo&&n!==Ui&&i!==Ui&&t.equal(n,i)}catch(o){i=Ui,t.error=o}finally{Kr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function IT(){throw new Error}var v0=IT;function y0(t){v0(t)}function mp(t){v0=t}var NT=null;function pp(t,n){let e=Object.create(Rc);e.value=t,n!==void 0&&(e.equal=n);let i=()=>b0(e);return i[_t]=e,Tc(e),[i,a=>jo(e,a),a=>iu(e,a)]}function b0(t){return Yr(t),t.value}function jo(t,n){fp()||y0(t),t.equal(t.value,n)||(t.value=n,TT(t))}function iu(t,n){fp()||y0(t),jo(t,n(t.value))}var Rc=re(C({},qr),{equal:kc,value:void 0,kind:"signal"});function TT(t){t.version++,m0(),up(t),NT?.(t)}var gp=re(C({},qr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function _p(t){if(t.dirty=!1,t.version>0&&!Za(t))return;t.version++;let n=hr(t);try{t.cleanup(),t.fn()}finally{Kr(t,n)}}var vp;function ru(){return vp}function Hi(t){let n=vp;return vp=t,n}var C0=Symbol("NotFound");function Xa(t){return t===C0||t?.name==="\u0275NotFound"}function yp(t,n,e){let i=Object.create(kT);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(Vo(i),Yr(i),i.value===Ui)throw i.error;return i.value};return o[_t]=i,Tc(i),o}function bp(t,n){Vo(t),jo(t,n),Ka(t)}function w0(t,n){if(Vo(t),t.value===Ui)throw t.error;iu(t,n),Ka(t)}var kT=re(C({},qr),{value:Fo,dirty:!0,error:null,equal:kc,kind:"linkedSignal",producerMustRecompute(t){return t.value===Fo||t.value===Lo},producerRecomputeValue(t){if(t.value===Lo)throw new Error("");let n=t.value;t.value=Lo;let e=hr(t),i,r=!1;try{let o=t.source(),a=n!==Fo&&n!==Ui,s=a?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,s),t.sourceValue=o,ce(null),r=a&&i!==Ui&&t.equal(n,i)}catch(o){i=Ui,t.error=o}finally{Kr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function S0(t){let n=ce(null);try{return t()}finally{ce(n)}}function ve(t){return typeof t=="function"}function Ja(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var ou=Ja(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Bo(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var ue=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(ve(i))try{i()}catch(o){n=o instanceof ou?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{D0(o)}catch(a){n=n??[],a instanceof ou?n=[...n,...a.errors]:n.push(a)}}if(n)throw new ou(n)}}add(n){var e;if(n&&n!==this)if(this.closed)D0(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Bo(e,n)}remove(n){let{_finalizers:e}=this;e&&Bo(e,n),n instanceof t&&n._removeParent(this)}};ue.EMPTY=(()=>{let t=new ue;return t.closed=!0,t})();var Cp=ue.EMPTY;function au(t){return t instanceof ue||t&&"closed"in t&&ve(t.remove)&&ve(t.add)&&ve(t.unsubscribe)}function D0(t){ve(t)?t():t.unsubscribe()}var _i={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var es={setTimeout(t,n,...e){let{delegate:i}=es;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=es;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function su(t){es.setTimeout(()=>{let{onUnhandledError:n}=_i;if(n)n(t);else throw t})}function Oc(){}var E0=wp("C",void 0,void 0);function x0(t){return wp("E",void 0,t)}function M0(t){return wp("N",t,void 0)}function wp(t,n,e){return{kind:t,value:n,error:e}}var Uo=null;function ts(t){if(_i.useDeprecatedSynchronousErrorHandling){let n=!Uo;if(n&&(Uo={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Uo;if(Uo=null,e)throw i}}else t()}function I0(t){_i.useDeprecatedSynchronousErrorHandling&&Uo&&(Uo.errorThrown=!0,Uo.error=t)}var Ho=class extends ue{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,au(n)&&n.add(this)):this.destination=OT}static create(n,e,i){return new mr(n,e,i)}next(n){this.isStopped?Dp(M0(n),this):this._next(n)}error(n){this.isStopped?Dp(x0(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Dp(E0,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},AT=Function.prototype.bind;function Sp(t,n){return AT.call(t,n)}var Ep=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){cu(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){cu(i)}else cu(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){cu(e)}}},mr=class extends Ho{constructor(n,e,i){super();let r;if(ve(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&_i.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Sp(n.next,o),error:n.error&&Sp(n.error,o),complete:n.complete&&Sp(n.complete,o)}):r=n}this.destination=new Ep(r)}};function cu(t){_i.useDeprecatedSynchronousErrorHandling?I0(t):su(t)}function RT(t){throw t}function Dp(t,n){let{onStoppedNotification:e}=_i;e&&es.setTimeout(()=>e(t,n))}var OT={closed:!0,next:Oc,error:RT,complete:Oc};var ns=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Rn(t){return t}function lu(...t){return xp(t)}function xp(t){return t.length===0?Rn:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var he=class t{constructor(n){n&&(this._subscribe=n)}lift(n){let e=new t;return e.source=this,e.operator=n,e}subscribe(n,e,i){let r=FT(n)?n:new mr(n,e,i);return ts(()=>{let{operator:o,source:a}=this;r.add(o?o.call(r,a):a?this._subscribe(r):this._trySubscribe(r))}),r}_trySubscribe(n){try{return this._subscribe(n)}catch(e){n.error(e)}}forEach(n,e){return e=N0(e),new e((i,r)=>{let o=new mr({next:a=>{try{n(a)}catch(s){r(s),o.unsubscribe()}},error:r,complete:i});this.subscribe(o)})}_subscribe(n){var e;return(e=this.source)===null||e===void 0?void 0:e.subscribe(n)}[ns](){return this}pipe(...n){return xp(n)(this)}toPromise(n){return n=N0(n),new n((e,i)=>{let r;this.subscribe(o=>r=o,o=>i(o),()=>e(r))})}};he.create=t=>new he(t);function N0(t){var n;return(n=t??_i.Promise)!==null&&n!==void 0?n:Promise}function PT(t){return t&&ve(t.next)&&ve(t.error)&&ve(t.complete)}function FT(t){return t&&t instanceof Ho||PT(t)&&au(t)}function LT(t){return ve(t?.lift)}function ye(t){return n=>{if(LT(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function we(t,n,e,i,r){return new Mp(t,n,e,i,r)}var Mp=class extends Ho{constructor(n,e,i,r,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(c){n.error(c)}}:super._next,this._error=r?function(s){try{r(s)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var T0=Ja(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var N=class extends he{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let e=new du(this,this);return e.operator=n,e}_throwIfClosed(){if(this.closed)throw new T0}next(n){ts(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let e of this.currentObservers)e.next(n)}})}error(n){ts(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:e}=this;for(;e.length;)e.shift().error(n)}})}complete(){ts(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:e,isStopped:i,observers:r}=this;return e||i?Cp:(this.currentObservers=null,r.push(n),new ue(()=>{this.currentObservers=null,Bo(r,n)}))}_checkFinalizedStatuses(n){let{hasError:e,thrownError:i,isStopped:r}=this;e?n.error(i):r&&n.complete()}asObservable(){let n=new he;return n.source=this,n}};N.create=(t,n)=>new du(t,n);var du=class extends N{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:Cp}};var At=class extends N{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Pc={now(){return(Pc.delegate||Date).now()},delegate:void 0};var uu=class extends N{constructor(n=1/0,e=1/0,i=Pc){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:a}=this;e||(i.push(n),!r&&i.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let a=0;a<o.length&&!n.closed;a+=i?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let a=e.now(),s=0;for(let c=1;c<i.length&&i[c]<=a;c+=2)s=c;s&&i.splice(0,s+1)}}};var fu=class extends ue{constructor(n,e){super()}schedule(n,e=0){return this}};var Fc={setInterval(t,n,...e){let{delegate:i}=Fc;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Fc;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var hu=class extends fu{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Fc.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Fc.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Bo(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Ip=(()=>{class t{constructor(e,i=t.now){this.schedulerActionCtor=e,this.now=i}schedule(e,i=0,r){return new this.schedulerActionCtor(this,e).schedule(r,i)}}return t.now=Pc.now,t})();var mu=class extends Ip{constructor(n,e=Ip.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Lc=new mu(hu),k0=Lc;var vt=new he(t=>t.complete());function pu(t){return t&&ve(t.schedule)}function Np(t){return t[t.length-1]}function gu(t){return ve(Np(t))?t.pop():void 0}function zi(t){return pu(Np(t))?t.pop():void 0}function A0(t,n){return typeof Np(t)=="number"?t.pop():n}function O0(t,n,e,i){function r(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(d){try{l(i.next(d))}catch(f){a(f)}}function c(d){try{l(i.throw(d))}catch(f){a(f)}}function l(d){d.done?o(d.value):r(d.value).then(s,c)}l((i=i.apply(t,n||[])).next())})}function R0(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function zo(t){return this instanceof zo?(this.v=t,this):new zo(t)}function P0(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),r[Symbol.asyncIterator]=function(){return this},r;function a(_){return function(b){return Promise.resolve(b).then(_,f)}}function s(_,b){i[_]&&(r[_]=function(M){return new Promise(function(x,E){o.push([_,M,x,E])>1||c(_,M)})},b&&(r[_]=b(r[_])))}function c(_,b){try{l(i[_](b))}catch(M){p(o[0][3],M)}}function l(_){_.value instanceof zo?Promise.resolve(_.value.v).then(d,f):p(o[0][2],_)}function d(_){c("next",_)}function f(_){c("throw",_)}function p(_,b){_(b),o.shift(),o.length&&c(o[0][0],o[0][1])}}function F0(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof R0=="function"?R0(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(a){return new Promise(function(s,c){a=t[o](a),r(s,c,a.done,a.value)})}}function r(o,a,s,c){Promise.resolve(c).then(function(l){o({value:l,done:s})},a)}}var _u=(t=>t&&typeof t.length=="number"&&typeof t!="function");function vu(t){return ve(t?.then)}function yu(t){return ve(t[ns])}function bu(t){return Symbol.asyncIterator&&ve(t?.[Symbol.asyncIterator])}function Cu(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function VT(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var wu=VT();function Su(t){return ve(t?.[wu])}function Du(t){return P0(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield zo(e.read());if(r)return yield zo(void 0);yield yield zo(i)}}finally{e.releaseLock()}})}function Eu(t){return ve(t?.getReader)}function et(t){if(t instanceof he)return t;if(t!=null){if(yu(t))return jT(t);if(_u(t))return BT(t);if(vu(t))return UT(t);if(bu(t))return L0(t);if(Su(t))return HT(t);if(Eu(t))return zT(t)}throw Cu(t)}function jT(t){return new he(n=>{let e=t[ns]();if(ve(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function BT(t){return new he(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function UT(t){return new he(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,su)})}function HT(t){return new he(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function L0(t){return new he(n=>{$T(t,n).catch(e=>n.error(e))})}function zT(t){return L0(Du(t))}function $T(t,n){var e,i,r,o;return O0(this,void 0,void 0,function*(){try{for(e=F0(t);i=yield e.next(),!i.done;){let a=i.value;if(n.next(a),n.closed)return}}catch(a){r={error:a}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function vn(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function xu(t,n=0){return ye((e,i)=>{e.subscribe(we(i,r=>vn(i,t,()=>i.next(r),n),()=>vn(i,t,()=>i.complete(),n),r=>vn(i,t,()=>i.error(r),n)))})}function Mu(t,n=0){return ye((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function V0(t,n){return et(t).pipe(Mu(n),xu(n))}function j0(t,n){return et(t).pipe(Mu(n),xu(n))}function B0(t,n){return new he(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function U0(t,n){return new he(e=>{let i;return vn(e,n,()=>{i=t[wu](),vn(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(a){e.error(a);return}o?e.complete():e.next(r)},0,!0)}),()=>ve(i?.return)&&i.return()})}function Iu(t,n){if(!t)throw new Error("Iterable cannot be null");return new he(e=>{vn(e,n,()=>{let i=t[Symbol.asyncIterator]();vn(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function H0(t,n){return Iu(Du(t),n)}function z0(t,n){if(t!=null){if(yu(t))return V0(t,n);if(_u(t))return B0(t,n);if(vu(t))return j0(t,n);if(bu(t))return Iu(t,n);if(Su(t))return U0(t,n);if(Eu(t))return H0(t,n)}throw Cu(t)}function ot(t,n){return n?z0(t,n):et(t)}function X(...t){let n=zi(t);return ot(t,n)}function Vc(t,n){let e=ve(t)?t:()=>t,i=r=>r.error(e());return new he(n?r=>n.schedule(i,0,r):i)}function jc(t){return!!t&&(t instanceof he||ve(t.lift)&&ve(t.subscribe))}var $o=Ja(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function $0(t){return t instanceof Date&&!isNaN(t)}function ge(t,n){return ye((e,i)=>{let r=0;e.subscribe(we(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:GT}=Array;function WT(t,n){return GT(n)?t(...n):t(n)}function Nu(t){return ge(n=>WT(t,n))}var{isArray:qT}=Array,{getPrototypeOf:YT,prototype:KT,keys:ZT}=Object;function Tu(t){if(t.length===1){let n=t[0];if(qT(n))return{args:n,keys:null};if(QT(n)){let e=ZT(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function QT(t){return t&&typeof t=="object"&&YT(t)===KT}function ku(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Bc(...t){let n=zi(t),e=gu(t),{args:i,keys:r}=Tu(t);if(i.length===0)return ot([],n);let o=new he(XT(i,n,r?a=>ku(r,a):Rn));return e?o.pipe(Nu(e)):o}function XT(t,n,e=Rn){return i=>{G0(n,()=>{let{length:r}=t,o=new Array(r),a=r,s=r;for(let c=0;c<r;c++)G0(n,()=>{let l=ot(t[c],n),d=!1;l.subscribe(we(i,f=>{o[c]=f,d||(d=!0,s--),s||i.next(e(o.slice()))},()=>{--a||i.complete()}))},i)},i)}}function G0(t,n,e){t?vn(e,t,n):n()}function W0(t,n,e,i,r,o,a,s){let c=[],l=0,d=0,f=!1,p=()=>{f&&!c.length&&!l&&n.complete()},_=M=>l<i?b(M):c.push(M),b=M=>{o&&n.next(M),l++;let x=!1;et(e(M,d++)).subscribe(we(n,E=>{r?.(E),o?_(E):n.next(E)},()=>{x=!0},void 0,()=>{if(x)try{for(l--;c.length&&l<i;){let E=c.shift();a?vn(n,a,()=>b(E)):b(E)}p()}catch(E){n.error(E)}}))};return t.subscribe(we(n,_,()=>{f=!0,p()})),()=>{s?.()}}function Zt(t,n,e=1/0){return ve(n)?Zt((i,r)=>ge((o,a)=>n(i,o,r,a))(et(t(i,r))),e):(typeof n=="number"&&(e=n),ye((i,r)=>W0(i,r,t,e)))}function Qr(t=1/0){return Zt(Rn,t)}function q0(){return Qr(1)}function Xr(...t){return q0()(ot(t,zi(t)))}function Go(t){return new he(n=>{et(t()).subscribe(n)})}function Uc(...t){let n=gu(t),{args:e,keys:i}=Tu(t),r=new he(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),c=a,l=a;for(let d=0;d<a;d++){let f=!1;et(e[d]).subscribe(we(o,p=>{f||(f=!0,l--),s[d]=p},()=>c--,void 0,()=>{(!c||!f)&&(l||o.next(i?ku(i,s):s),o.complete())}))}});return n?r.pipe(Nu(n)):r}function Y0(t=0,n,e=k0){let i=-1;return n!=null&&(pu(n)?e=n:i=n),new he(r=>{let o=$0(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){r.closed||(r.next(a++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Rt(...t){let n=zi(t),e=A0(t,1/0),i=t;return i.length?i.length===1?et(i[0]):Qr(e)(ot(i,n)):vt}function je(t,n){return ye((e,i)=>{let r=0;e.subscribe(we(i,o=>t.call(n,o,r++)&&i.next(o)))})}function K0(t){return ye((n,e)=>{let i=!1,r=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,e.next(l)}a&&e.complete()},c=()=>{o=null,a&&e.complete()};n.subscribe(we(e,l=>{i=!0,r=l,o||et(t(l)).subscribe(o=we(e,s,c))},()=>{a=!0,(!i||!o||o.closed)&&e.complete()}))})}function Au(t,n=Lc){return K0(()=>Y0(t,n))}function Jr(t){return ye((n,e)=>{let i=null,r=!1,o;i=n.subscribe(we(e,void 0,void 0,a=>{o=et(t(a,Jr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Wo(t,n){return ve(n)?Zt(t,n,1):Zt(t,1)}function Hc(t,n=Lc){return ye((e,i)=>{let r=null,o=null,a=null,s=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=a+t,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}s()}e.subscribe(we(i,l=>{o=l,a=n.now(),r||(r=n.schedule(c,t),i.add(r))},()=>{s(),i.complete()},void 0,()=>{o=r=null}))})}function Z0(t){return ye((n,e)=>{let i=!1;n.subscribe(we(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Ot(t){return t<=0?()=>vt:ye((n,e)=>{let i=0;n.subscribe(we(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Ru(t,n=Rn){return t=t??JT,ye((e,i)=>{let r,o=!0;e.subscribe(we(i,a=>{let s=n(a);(o||!t(r,s))&&(o=!1,r=s,i.next(a))}))})}function JT(t,n){return t===n}function Q0(t=ek){return ye((n,e)=>{let i=!1;n.subscribe(we(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function ek(){return new $o}function qo(t){return ye((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function pr(t,n){let e=arguments.length>=2;return i=>i.pipe(t?je((r,o)=>t(r,o,i)):Rn,Ot(1),e?Z0(n):Q0(()=>new $o))}function Ou(t){return t<=0?()=>vt:ye((n,e)=>{let i=[];n.subscribe(we(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Pu(){return ye((t,n)=>{let e,i=!1;t.subscribe(we(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function zc(t={}){let{connector:n=()=>new N,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let a,s,c,l=0,d=!1,f=!1,p=()=>{s?.unsubscribe(),s=void 0},_=()=>{p(),a=c=void 0,d=f=!1},b=()=>{let M=a;_(),M?.unsubscribe()};return ye((M,x)=>{l++,!f&&!d&&p();let E=c=c??n();x.add(()=>{l--,l===0&&!f&&!d&&(s=Tp(b,r))}),E.subscribe(x),!a&&l>0&&(a=new mr({next:L=>E.next(L),error:L=>{f=!0,p(),s=Tp(_,e,L),E.error(L)},complete:()=>{d=!0,p(),s=Tp(_,i),E.complete()}}),et(M).subscribe(a))})(o)}}function Tp(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new mr({next:()=>{i.unsubscribe(),t()}});return et(n(...e)).subscribe(i)}function Fu(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,zc({connector:()=>new uu(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function $c(t){return je((n,e)=>t<=e)}function dt(...t){let n=zi(t);return ye((e,i)=>{(n?Xr(t,e,n):Xr(t,e)).subscribe(i)})}function ht(t,n){return ye((e,i)=>{let r=null,o=0,a=!1,s=()=>a&&!r&&i.complete();e.subscribe(we(i,c=>{r?.unsubscribe();let l=0,d=o++;et(t(c,d)).subscribe(r=we(i,f=>i.next(n?n(c,f,d,l++):f),()=>{r=null,s()}))},()=>{a=!0,s()}))})}function Ee(t){return ye((n,e)=>{et(t).subscribe(we(e,()=>e.complete(),Oc)),!e.closed&&n.subscribe(e)})}function kp(t,n=!1){return ye((e,i)=>{let r=0;e.subscribe(we(i,o=>{let a=t(o,r++);(a||n)&&i.next(o),!a&&i.complete()}))})}function Bt(t,n,e){let i=ve(t)||n||e?{next:t,error:n,complete:e}:t;return i?ye((r,o)=>{var a;(a=i.subscribe)===null||a===void 0||a.call(i);let s=!0;r.subscribe(we(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;s=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;s=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;s&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):Rn}var zu="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",k=class extends Error{code;constructor(n,e){super(Gi(n,e)),this.code=n}};function tk(t){return`NG0${Math.abs(t)}`}function Gi(t,n){return`${tk(t)}${n?": "+n:""}`}function He(t){for(let n in t)if(t[n]===He)return n;throw Error("")}function iC(t,n){for(let e in n)Object.hasOwn(n,e)&&!Object.hasOwn(t,e)&&(t[e]=n[e])}function Qc(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Qc).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function $u(t,n){return t?n?`${t} ${n}`:t:n||""}var nk=He({__forward_ref__:He});function Xt(t){return t.__forward_ref__=Xt,t}function $t(t){return Gp(t)?t():t}function Gp(t){return typeof t=="function"&&Object.hasOwn(t,nk)&&t.__forward_ref__===Xt}function me(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function ne(t){return{providers:t.providers||[],imports:t.imports||[]}}function Xc(t){return ik(t,Gu)}function Wp(t){return Xc(t)!==null}function ik(t,n){return Object.hasOwn(t,n)&&t[n]||null}function rk(t){let n=t?.[Gu]??null;return n||null}function Rp(t){return t&&Object.hasOwn(t,Vu)?t[Vu]:null}var Gu=He({\u0275prov:He}),Vu=He({\u0275inj:He}),w=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=me({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function qp(t){return t&&!!t.\u0275providers}var Jc=He({\u0275cmp:He}),el=He({\u0275dir:He}),Yp=He({\u0275pipe:He}),Kp=He({\u0275mod:He}),qc=He({\u0275fac:He}),Xo=He({__NG_ELEMENT_ID__:He}),X0=He({__NG_ENV_ID__:He});function rC(t){return Wu(t,"@NgModule"),t[Kp]||null}function no(t){return Wu(t,"@Component"),t[Jc]||null}function Zp(t){return Wu(t,"@Directive"),t[el]||null}function oC(t){return Wu(t,"@Pipe"),t[Yp]||null}function Wu(t,n){if(t==null)throw new k(-919,!1)}function as(t){return typeof t=="string"?t:t==null?"":String(t)}var aC=He({ngErrorCode:He}),ok=He({ngErrorMessage:He}),ak=He({ngTokenPath:He});function Qp(t,n){return sC("",-200,n)}function qu(t,n){throw new k(-201,!1)}function sC(t,n,e){let i=new k(n,t);return i[aC]=n,i[ok]=t,e&&(i[ak]=e),i}function sk(t){return t[aC]}var Op;function cC(){return Op}function cn(t){let n=Op;return Op=t,n}function Xp(t,n,e){let i=Xc(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;qu(t,"")}var Qn=globalThis;var ck={},Yo=ck,lk="__NG_DI_FLAG__",Pp=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Ko(e)||0;try{return this.injector.get(n,i&8?null:Yo,i)}catch(r){if(Xa(r))return r;throw r}}};function dk(t,n=0){let e=ru();if(e===void 0)throw new k(-203,!1);if(e===null)return Xp(t,void 0,n);{let i=uk(n),r=e.retrieve(t,i);if(Xa(r)){if(i.optional)return null;throw r}return r}}function J(t,n=0){return(cC()||dk)($t(t),n)}function u(t,n){return J(t,Ko(n))}function Ko(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function uk(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Fp(t){let n=[];for(let e=0;e<t.length;e++){let i=$t(t[e]);if(Array.isArray(i)){if(i.length===0)throw new k(900,!1);let r,o=0;for(let a=0;a<i.length;a++){let s=i[a],c=fk(s);typeof c=="number"?c===-1?r=s.token:o|=c:r=s}n.push(J(r,o))}else n.push(J(i))}return n}function fk(t){return t[lk]}function to(t,n){let e=Object.hasOwn(t,qc);return e?t[qc]:null}function lC(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function dC(t){return t.flat(Number.POSITIVE_INFINITY)}function Yu(t,n){t.forEach(e=>Array.isArray(e)?Yu(e,n):n(e))}function Jp(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function tl(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function uC(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function fC(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Ku(t,n,e){let i=ss(t,n);return i>=0?t[i|1]=e:(i=~i,fC(t,i,n,e)),i}function Zu(t,n){let e=ss(t,n);if(e>=0)return t[e|1]}function ss(t,n){return hk(t,n,1)}function hk(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),a=t[o<<e];if(n===a)return o<<e;a>n?r=o:i=o+1}return~(r<<e)}var io={},Qt=[],Jo=new w(""),nl=new w("",-1),eg=new w(""),rs=class{get(n,e=Yo){if(e===Yo){let r=sC("",-201);throw r.name="\u0275NotFound",r}return e}};function Wi(t){return{\u0275providers:t}}function hC(t){return Wi([{provide:Jo,multi:!0,useValue:t}])}function mC(...t){return{\u0275providers:tg(!0,t),\u0275fromNgModule:!0}}function tg(t,...n){let e=[],i=new Set,r,o=a=>{e.push(a)};return Yu(n,a=>{let s=a;ju(s,o,[],i)&&(r||=[],r.push(s))}),r!==void 0&&pC(r,o),e}function pC(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];ng(r,o=>{n(o,i)})}}function ju(t,n,e,i){if(t=$t(t),!t)return!1;let r=null,o=Rp(t),a=!o&&no(t);if(!o&&!a){let c=t.ngModule;if(o=Rp(c),o)r=c;else return!1}else{if(a&&!a.standalone)return!1;r=t}let s=i.has(r);if(a){if(s)return!1;if(i.add(r),a.dependencies){let c=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let l of c)ju(l,n,e,i)}}else if(o){if(o.imports!=null&&!s){i.add(r);let l;Yu(o.imports,d=>{ju(d,n,e,i)&&(l||=[],l.push(d))}),l!==void 0&&pC(l,n)}if(!s){let l=to(r)||(()=>new r);n({provide:r,useFactory:l,deps:Qt},r),n({provide:eg,useValue:r,multi:!0},r),n({provide:Jo,useValue:()=>J(r),multi:!0},r)}let c=o.providers;if(c!=null&&!s){let l=t;ng(c,d=>{n(d,l)})}}else return!1;return r!==t&&t.providers!==void 0}function ng(t,n){for(let e of t)qp(e)&&(e=e.\u0275providers),Array.isArray(e)?ng(e,n):n(e)}var mk=He({provide:String,useValue:He});function gC(t){return t!==null&&typeof t=="object"&&mk in t}function pk(t){return!!(t&&t.useExisting)}function gk(t){return!!(t&&t.useFactory)}function Zo(t){return typeof t=="function"}function _C(t){return!!t.useClass}var il=new w(""),Lu={},J0={},Ap;function cs(){return Ap===void 0&&(Ap=new rs),Ap}var qe=class{},Qo=class extends qe{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Vp(n,a=>this.processProvider(a)),this.records.set(nl,is(void 0,this)),r.has("environment")&&this.records.set(qe,is(void 0,this));let o=this.records.get(il);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(eg,Qt,{self:!0}))}retrieve(n,e){let i=Ko(e)||0;try{return this.get(n,Yo,i)}catch(r){if(Xa(r))return r;throw r}}destroy(){Gc(this),this._destroyed=!0;let n=ce(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),ce(n)}}onDestroy(n){return Gc(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Gc(this);let e=Hi(this),i=cn(void 0),r;try{return n()}finally{Hi(e),cn(i)}}get(n,e=Yo,i){if(Gc(this),Object.hasOwn(n,X0))return n[X0](this);let r=Ko(i),o,a=Hi(this),s=cn(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=Ck(n)&&Xc(n);d&&this.injectableDefInScope(d)?l=is(Lp(n),Lu):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?cs():this.parent;return e=r&8&&e===Yo?null:e,c.get(n,e)}catch(c){let l=sk(c);throw l===-200||l===-201?new k(l,null):c}finally{cn(s),Hi(a)}}resolveInjectorInitializers(){let n=ce(null),e=Hi(this),i=cn(void 0),r;try{let o=this.get(Jo,Qt,{self:!0});for(let a of o)a()}finally{Hi(e),cn(i),ce(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=$t(n);let e=Zo(n)?n:$t(n&&n.provide),i=vk(n);if(!Zo(n)&&n.multi===!0){let r=this.records.get(e);r||(r=is(void 0,Lu,!0),r.factory=()=>Fp(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=ce(null);try{if(e.value===J0)throw Qp("");return e.value===Lu&&(e.value=J0,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&bk(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{ce(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=$t(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Lp(t){let n=Xc(t),e=n!==null?n.factory:to(t);if(e!==null)return e;if(t instanceof w)throw new k(-204,!1);if(t instanceof Function)return _k(t);throw new k(-204,!1)}function _k(t){if(t.length>0)throw new k(-204,!1);let e=rk(t);return e!==null?()=>e.factory(t):()=>new t}function vk(t){if(gC(t))return is(void 0,t.useValue);{let n=ig(t);return is(n,Lu)}}function ig(t,n,e){let i;if(Zo(t)){let r=$t(t);return to(r)||Lp(r)}else if(gC(t))i=()=>$t(t.useValue);else if(gk(t))i=()=>t.useFactory(...Fp(t.deps||[]));else if(pk(t))i=(r,o)=>J($t(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=$t(t&&(t.useClass||t.provide));if(yk(t))i=()=>new r(...Fp(t.deps));else return to(r)||Lp(r)}return i}function Gc(t){if(t.destroyed)throw new k(-205,!1)}function is(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function yk(t){return!!t.deps}function bk(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function Ck(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Vp(t,n){for(let e of t)Array.isArray(e)?Vp(e,n):e&&qp(e)?Vp(e.\u0275providers,n):n(e)}function Ft(t,n){let e;t instanceof Qo?(Gc(t),e=t):e=new Pp(t);let i,r=Hi(e),o=cn(void 0);try{return n()}finally{Hi(r),cn(o)}}function vC(){return cC()!==void 0||ru()!=null}var vi=0,oe=1,fe=2,Pt=3,Xn=4,Jt=5,ea=6,ls=7,yt=8,qi=9,yi=10,Xe=11,ds=12,rg=13,ro=14,dn=15,oo=16,ta=17,Yi=18,Ki=19,og=20,gr=21,Qu=22,_r=23,On=24,na=25,Zi=26,ut=27,yC=1,ag=6,ia=7,rl=8,ra=9,ft=10;function yr(t){return Array.isArray(t)&&typeof t[yC]=="object"}function Jn(t){return Array.isArray(t)&&t[yC]===!0}function sg(t){return(t.flags&4)!==0}function br(t){return t.componentOffset>-1}function ol(t){return(t.flags&1)===1}function Qi(t){return!!t.template}function us(t){return(t[fe]&512)!==0}function oa(t){return(t[fe]&256)===256}var Oe=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(Oe||{}),Wc,os="svg",Xu="math",bC="",eC="*",jp=()=>Object.create(null);function wk(){return Wc||(Wc=jp(),eo(Oe.HTML,void 0,[["iframe",["srcdoc"]],["*",["innerHTML","outerHTML"]]]),eo(Oe.STYLE,void 0,[["*",["style"]]]),eo(Oe.URL,void 0,[["*",["formAction"]],["area",["href"]],["a",["href","xlink:href"]],["form",["action"]],["img",["src"]],["video",["src"]]]),eo(Oe.URL,Xu,[["*",["href","xlink:href"]]]),eo(Oe.RESOURCE_URL,void 0,[["base",["href"]],["embed",["src"]],["frame",["src"]],["iframe",["src"]],["link",["href"]],["object",["codebase","data"]]]),eo(Oe.URL,os,[["a",["href","xlink:href"]]]),eo(Oe.ATTRIBUTE_NO_BINDING,os,[["animate",["attributeName","values","to","from"]],["set",["to","attributeName"]],["animateMotion",["attributeName"]],["animateTransform",["attributeName"]]]),eo(Oe.ATTRIBUTE_NO_BINDING,void 0,[["unknown",["attributeName","values","to","from","sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]],["iframe",["sandbox","allow","allowFullscreen","referrerPolicy","csp","fetchPriority","credentialless"]]]),Wc)}function eo(t,n,e){let i=n??bC;for(let[r,o]of e){let a=r.toLowerCase();for(let s of o){let c=s.toLowerCase(),l=Wc[c]??=jp(),d=l[i]??=jp();d[a]=t}}}function CC(t,n,e){let r=wk()[n.toLowerCase()];if(!r)return Oe.NONE;let o=t.toLowerCase(),a;if(e){let s=r[e];s&&(a=s[o]??s[eC])}if(a===void 0){let s=r[bC];s&&(a=s[o]??s[eC])}return a??Oe.NONE}function Gt(t){for(;Array.isArray(t);)t=t[vi];return t}function cg(t,n){return Gt(n[t])}function Pn(t,n){return Gt(n[t.index])}function Ju(t,n){return t.data[n]}function lg(t,n){return t[n]}function dg(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function ei(t,n){let e=n[t];return yr(e)?e:e[vi]}function wC(t){return(t[fe]&4)===4}function ef(t){return(t[fe]&128)===128}function SC(t){return Jn(t[Pt])}function Fn(t,n){return n==null?null:t[n]}function ug(t){t[ta]=0}function fg(t){t[fe]&1024||(t[fe]|=1024,ef(t)&&aa(t))}function DC(t,n){for(;t>0;)n=n[ro],t--;return n}function al(t){return!!(t[fe]&9216||t[On]?.dirty)}function tf(t){t[yi].changeDetectionScheduler?.notify(8),t[fe]&64&&(t[fe]|=1024),al(t)&&aa(t)}function aa(t){t[yi].changeDetectionScheduler?.notify(0);let n=vr(t);for(;n!==null&&!(n[fe]&8192||(n[fe]|=8192,!ef(n)));)n=vr(n)}function nf(t,n){if(oa(t))throw new k(911,!1);t[gr]===null&&(t[gr]=[]),t[gr].push(n)}function EC(t,n){if(t[gr]===null)return;let e=t[gr].indexOf(n);e!==-1&&t[gr].splice(e,1)}function vr(t){let n=t[Pt];return Jn(n)?n[Pt]:n}function hg(t){return t[ls]??=[]}function mg(t){return t.cleanup??=[]}function xC(t,n,e,i){let r=hg(n);r.push(e),t.firstCreatePass&&mg(t).push(i,r.length-1)}var be={lFrame:VC(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Bp=!1;function MC(){return be.lFrame.elementDepthCount}function IC(){be.lFrame.elementDepthCount++}function pg(){be.lFrame.elementDepthCount--}function gg(){return be.bindingsEnabled}function _g(){return be.skipHydrationRootTNode!==null}function vg(t){return be.skipHydrationRootTNode===t}function yg(){be.skipHydrationRootTNode=null}function le(){return be.lFrame.lView}function tt(){return be.lFrame.tView}function q(t){return be.lFrame.contextLView=t,t[yt]}function Y(t){return be.lFrame.contextLView=null,t}function xt(){let t=bg();for(;t!==null&&t.type===64;)t=t.parent;return t}function bg(){return be.lFrame.currentTNode}function NC(){let t=be.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function fs(t,n){let e=be.lFrame;e.currentTNode=t,e.isParent=n}function Cg(){return be.lFrame.isParent}function wg(){be.lFrame.isParent=!1}function TC(){return be.lFrame.contextLView}function Sg(){return Bp}function Yc(t){let n=Bp;return Bp=t,n}function rf(){let t=be.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function kC(){return be.lFrame.bindingIndex}function AC(t){return be.lFrame.bindingIndex=t}function ao(){return be.lFrame.bindingIndex++}function of(t){let n=be.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function RC(){return be.lFrame.inI18n}function OC(t,n){let e=be.lFrame;e.bindingIndex=e.bindingRootIndex=t,af(n)}function PC(){return be.lFrame.currentDirectiveIndex}function af(t){be.lFrame.currentDirectiveIndex=t}function FC(t){let n=be.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function sf(){return be.lFrame.currentQueryIndex}function sl(t){be.lFrame.currentQueryIndex=t}function Sk(t){let n=t[oe];return n.type===2?n.declTNode:n.type===1?t[Jt]:null}function Dg(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=Sk(o),r===null||(o=o[ro],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=be.lFrame=LC();return i.currentTNode=n,i.lView=t,!0}function cf(t){let n=LC(),e=t[oe];be.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function LC(){let t=be.lFrame,n=t===null?null:t.child;return n===null?VC(t):n}function VC(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function jC(){let t=be.lFrame;return be.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Eg=jC;function lf(){let t=jC();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function BC(t){return(be.lFrame.contextLView=DC(t,be.lFrame.contextLView))[yt]}function bi(){return be.lFrame.selectedIndex}function so(t){be.lFrame.selectedIndex=t}function sa(){let t=be.lFrame;return Ju(t.tView,t.selectedIndex)}function yn(){be.lFrame.currentNamespace=os}function ca(){Dk()}function Dk(){be.lFrame.currentNamespace=null}function xg(){return be.lFrame.currentNamespace}var UC=!0;function df(){return UC}function uf(t){UC=t}function Up(t,n=null,e=null,i){let r=Mg(t,n,e,i);return r.resolveInjectorInitializers(),r}function Mg(t,n=null,e=null,i,r=new Set){let o=[e||Qt,mC(t)],a;return new Qo(o,n||cs(),a||null,r)}var te=class t{static THROW_IF_NOT_FOUND=Yo;static NULL=new rs;static create(n,e){if(Array.isArray(n))return Up({name:""},e,n,"");{let i=n.name??"";return Up({name:i},n.parent,n.providers,i)}}static \u0275prov=me({token:t,providedIn:"any",factory:()=>J(nl)});static __NG_ELEMENT_ID__=-1},$=new w(""),Et=class{static __NG_ELEMENT_ID__=Ek;static __NG_ENV_ID__=n=>n},Bu=class extends Et{_lView;constructor(n){super(),this._lView=n}get destroyed(){return oa(this._lView)}onDestroy(n){let e=this._lView;return nf(e,n),()=>EC(e,n)}};function Ek(){return new Bu(le())}var HC=!1,zC=new w(""),Cr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new At(!1);debugTaskTracker=u(zC,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new he(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=me({token:t,providedIn:"root",factory:()=>new t})}return t})(),Hp=class extends N{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,vC()&&(this.destroyRef=u(Et,{optional:!0})??void 0,this.pendingTasks=u(Cr,{optional:!0})??void 0)}emit(n){let e=ce(null);try{super.next(n)}finally{ce(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),a=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),a=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:r,error:o,complete:a});return n instanceof ue&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},A=Hp;function Uu(...t){}function Ig(t){let n,e;function i(){t=Uu;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function $C(t){return queueMicrotask(()=>t()),()=>{t=Uu}}var Ng="isAngularZone",Kc=Ng+"_ID",xk=0,H=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new A(!1);onMicrotaskEmpty=new A(!1);onStable=new A(!1);onError=new A(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=HC}=n;if(typeof Zone>"u")throw new k(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!r&&i,a.shouldCoalesceRunChangeDetection=r,a.callbackScheduled=!1,a.scheduleInRootZone=o,Nk(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Ng)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new k(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new k(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+r,n,Mk,Uu,Uu);try{return o.runTask(a,e,i)}finally{o.cancelTask(a)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},Mk={};function Tg(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function Ik(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Ig(()=>{t.callbackScheduled=!1,zp(t),t.isCheckStableRunning=!0,Tg(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),zp(t)}function Nk(t){let n=()=>{Ik(t)},e=xk++;t._inner=t._inner.fork({name:"angular",properties:{[Ng]:!0,[Kc]:e,[Kc+e]:!0},onInvokeTask:(i,r,o,a,s,c)=>{if(Tk(c))return i.invokeTask(o,a,s,c);try{return tC(t),i.invokeTask(o,a,s,c)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),nC(t)}},onInvoke:(i,r,o,a,s,c,l)=>{try{return tC(t),i.invoke(o,a,s,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!kk(c)&&n(),nC(t)}},onHasTask:(i,r,o,a)=>{i.hasTask(o,a),r===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,zp(t),Tg(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(i,r,o,a)=>(i.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function zp(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function tC(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function nC(t){t._nesting--,Tg(t)}var Zc=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new A;onMicrotaskEmpty=new A;onStable=new A;onError=new A;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function Tk(t){return GC(t,"__ignore_ng_zone__")}function kk(t){return GC(t,"__scheduler_tick__")}function GC(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var ln=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Ln=new w("",{factory:()=>{let t=u(H),n=u(qe),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(ln),e.handleError(i))})}}}),WC={provide:Jo,useValue:()=>{let t=u(ln,{optional:!0})},multi:!0},Ak=new w("",{factory:()=>{let t=u($).defaultView;if(!t)return;let n=u(Ln),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),u(Et).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function kg(){return Wi([hC(()=>{u(Ak)})])}function S(t,n){let[e,i,r]=pp(t,n?.equal),o=e,a=o[_t];return o.set=i,o.update=r,o.asReadonly=ff.bind(o),o}function ff(){let t=this[_t];if(t.readonlyFn===void 0){let n=()=>this();n[_t]=t,t.readonlyFn=n}return t.readonlyFn}var wr=new w("",{factory:()=>Rk}),Rk="ng";var hf=new w(""),la=new w("",{providedIn:"platform",factory:()=>"unknown"}),cl=new w(""),co=new w("",{factory:()=>u($).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var hs=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=Ok}return t})();function Ok(){return new hs(le(),xt())}var $i=class{},ll=new w("",{factory:()=>!0});var Ag=new w(""),mf=(()=>{class t{static \u0275prov=me({token:t,providedIn:"root",factory:()=>new $p})}return t})(),$p=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Hu=class{[_t];constructor(n){this[_t]=n}destroy(){this[_t].destroy()}};function Vn(t,n){let e=n?.injector??u(te),i=n?.manualCleanup!==!0?e.get(Et):null,r,o=e.get(hs,null,{optional:!0}),a=e.get($i);return o!==null?(r=Lk(o.view,a,t),i instanceof Bu&&i._lView===o.view&&(i=null)):r=Vk(t,e.get(mf),a),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Hu(r)}var qC=re(C({},gp),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=Yc(!1);try{_p(this)}finally{Yc(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=ce(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],ce(t)}}}),Pk=re(C({},qC),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Zr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),Fk=re(C({},qC),{consumerMarkedDirty(){this.view[fe]|=8192,aa(this.view),this.notifier.notify(13)},destroy(){if(Zr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[_r]?.delete(this)}});function Lk(t,n,e){let i=Object.create(Fk);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=YC(i,e),t[_r]??=new Set,t[_r].add(i),i.consumerMarkedDirty(i),i}function Vk(t,n,e){let i=Object.create(Pk);return i.fn=YC(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function YC(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function bn(t){return typeof t=="function"&&t[_t]!==void 0}function pf(t){return bn(t)&&typeof t.set=="function"}var dl=(()=>{class t{internalPendingTasks=u(Cr);scheduler=u($i);errorHandler=u(Ln);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=me({token:t,providedIn:"root",factory:()=>new t})}return t})();function Cl(t){return{toString:t}.toString()}var Ae=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Ae||{}),Df=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function Ow(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Pw=null,Pe=(()=>{Pw=KC;let t=()=>KC;return t.ngInherit=!0,t})();function qk(){return Pw}function KC(t){return t.type.prototype.ngOnChanges&&(t.setInput=Kk),Yk}function Yk(){let t=Fw(this),n=t?.current;if(n){let e=t.previous;if(e===io)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function Kk(t,n,e,i,r){let o=this.declaredInputs[i],a=Fw(t)||Zk(t,{previous:io,current:null}),s=a.current||(a.current={}),c=a.previous,l=c[o];s[o]=new Df(l&&l.currentValue,e,c===io),Ow(t,n,r,e)}var $g="__ngSimpleChanges__";function Fw(t){return Object.hasOwn(t,$g)&&t[$g]||null}function Zk(t,n){return t[$g]=n}var ZC=[];var ze=function(t,n=null,e){for(let i=0;i<ZC.length;i++){let r=ZC[i];r(t,n,e)}};function Qk(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let a=qk()(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function Lw(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function bf(t,n,e){Vw(t,n,3,e)}function Cf(t,n,e,i){(t[fe]&3)===e&&Vw(t,n,e,i)}function Rg(t,n){let e=t[fe];(e&3)===n&&(e&=16383,e+=1,t[fe]=e)}function Vw(t,n,e,i){let r=i!==void 0?t[ta]&65535:0,o=i??-1,a=n.length-1,s=0;for(let c=r;c<a;c++)if(typeof n[c+1]=="number"){if(s=n[c],i!=null&&s>=i)break}else n[c]<0&&(t[ta]+=65536),(s<o||o==-1)&&(Xk(t,e,n,c),t[ta]=(t[ta]&4294901760)+c+2),c++}function QC(t,n){ze(Ae.LifecycleHookStart,t,n);let e=ce(null);try{n.call(t)}finally{ce(e),ze(Ae.LifecycleHookEnd,t,n)}}function Xk(t,n,e,i){let r=e[i]<0,o=e[i+1],a=r?-e[i]:e[i],s=t[a];r?t[fe]>>14<t[ta]>>16&&(t[fe]&3)===n&&(t[fe]+=16384,QC(s,o)):QC(s,o)}var ps=-1,fa=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function Jk(t){return(t.flags&8)!==0}function eA(t){return(t.flags&16)!==0}function tA(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],a=e[i++],s=e[i++];t.setAttribute(n,a,s,o)}else{let o=r,a=e[++i];nA(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),i++}}return i}function jw(t){return t===3||t===4||t===6}function nA(t){return t.charCodeAt(0)===64}function gs(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?XC(t,e,r,null,n[++i]):XC(t,e,r,null,null))}}return t}function XC(t,n,e,i,r){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function Bw(t){return t!==ps}function Ef(t){return t&32767}function iA(t){return t>>16}function xf(t,n){let e=iA(t),i=n;for(;e>0;)i=i[ro],e--;return i}var Gg=!0;function Mf(t){let n=Gg;return Gg=t,n}var rA=256,Uw=rA-1,Hw=5,oA=0,Xi={};function aA(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:Object.hasOwn(e,Xo)&&(i=e[Xo]),i==null&&(i=e[Xo]=oA++);let r=i&Uw,o=1<<r;n.data[t+(r>>Hw)]|=o}function If(t,n){let e=zw(t,n);if(e!==-1)return e;let i=n[oe];i.firstCreatePass&&(t.injectorIndex=n.length,Og(i.data,t),Og(n,null),Og(i.blueprint,null));let r=E_(t,n),o=t.injectorIndex;if(Bw(r)){let a=Ef(r),s=xf(r,n),c=s[oe].data;for(let l=0;l<8;l++)n[o+l]=s[a+l]|c[a+l]}return n[o+8]=r,o}function Og(t,n){t.push(0,0,0,0,0,0,0,0,n)}function zw(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function E_(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=Yw(r),i===null)return ps;if(e++,r=r[ro],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return ps}function Wg(t,n,e){aA(t,n,e)}function sA(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(jw(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function $w(t,n,e){if(e&8||t!==void 0)return t;qu(n,"NodeInjector")}function Gw(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[qi],o=cn(void 0);try{return r?r.get(n,i,e&8):Xp(n,i,e&8)}finally{cn(o)}}return $w(i,n,e)}function Ww(t,n,e,i=0,r){if(t!==null){if(n[fe]&2048&&!(i&2)){let a=uA(t,n,e,i,Xi);if(a!==Xi)return a}let o=qw(t,n,e,i,Xi);if(o!==Xi)return o}return Gw(n,e,i,r)}function qw(t,n,e,i,r){let o=lA(e);if(typeof o=="function"){if(!Dg(n,t,i))return i&1?$w(r,e,i):Gw(n,e,i,r);try{let a;if(a=o(i),a==null&&!(i&8))qu(e);else return a}finally{Eg()}}else if(typeof o=="number"){let a=null,s=zw(t,n),c=ps,l=i&1?n[dn][Jt]:null;for((s===-1||i&4)&&(c=s===-1?E_(t,n):n[s+8],c===ps||!ew(i,!1)?s=-1:(a=n[oe],s=Ef(c),n=xf(c,n)));s!==-1;){let d=n[oe];if(JC(o,s,d.data)){let f=cA(s,n,e,a,i,l);if(f!==Xi)return f}c=n[s+8],c!==ps&&ew(i,n[oe].data[s+8]===l)&&JC(o,s,n)?(a=d,s=Ef(c),n=xf(c,n)):s=-1}}return r}function cA(t,n,e,i,r,o){let a=n[oe],s=a.data[t+8],c=i==null?br(s)&&Gg:i!=a&&(s.type&3)!==0,l=r&1&&o===s,d=wf(s,a,e,c,l);return d!==null?pl(n,a,d,s,r):Xi}function wf(t,n,e,i,r){let o=t.providerIndexes,a=n.data,s=o&1048575,c=t.directiveStart,l=t.directiveEnd,d=o>>20,f=i?s:s+d,p=r?s+d:l;for(let _=f;_<p;_++){let b=a[_];if(_<c&&e===b||_>=c&&b.type===e)return _}if(r){let _=a[c];if(_&&Qi(_)&&_.type===e)return c}return null}function pl(t,n,e,i,r){let o=t[e],a=n.data;if(o instanceof fa){let s=o;if(s.resolving)throw Qp("");let c=Mf(s.canSeeViewProviders);s.resolving=!0;let l=a[e].type||a[e],d,f=s.injectImpl?cn(s.injectImpl):null,p=Dg(t,i,0);try{o=t[e]=s.factory(void 0,r,a,t,i),n.firstCreatePass&&e>=i.directiveStart&&Qk(e,a[e],n)}finally{f!==null&&cn(f),Mf(c),s.resolving=!1,Eg()}}return o}function lA(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=Object.hasOwn(t,Xo)?t[Xo]:void 0;return typeof n=="number"?n>=0?n&Uw:dA:n}function JC(t,n,e){let i=1<<t;return!!(e[n+(t>>Hw)]&i)}function ew(t,n){return!(t&2)&&!(t&1&&n)}var lo=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return Ww(this._tNode,this._lView,n,Ko(i),e)}};function dA(){return new lo(xt(),le())}function en(t){return Cl(()=>{let n=t.prototype.constructor,e=n[qc]||qg(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[qc]||qg(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function qg(t){return Gp(t)?()=>{let n=qg($t(t));return n&&n()}:to(t)}function uA(t,n,e,i,r){let o=t,a=n;for(;o!==null&&a!==null&&a[fe]&2048&&!us(a);){let s=qw(o,a,e,i|2,Xi);if(s!==Xi)return s;let c=o.parent;if(!c){let l=a[og];if(l){let d=l.get(e,Xi,i&-5);if(d!==Xi)return d}c=Yw(a),a=a[ro]}o=c}return r}function Yw(t){let n=t[oe],e=n.type;return e===2?n.declTNode:e===1?t[Jt]:null}function wl(t){return sA(xt(),t)}function V(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function fA(){return ws(xt(),le())}function ws(t,n){return new U(Pn(t,n))}var U=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=fA}return t})();function Kw(t){return t instanceof U?t.nativeElement:t}function hA(){return this._results[Symbol.iterator]()}var Si=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new N}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=dC(n);(this._changesDetected=!lC(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=hA};function Zw(t){return(t.flags&128)===128}var x_=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(x_||{}),Qw=new Map,mA=0;function pA(){return mA++}function gA(t){Qw.set(t[Ki],t)}function Yg(t){Qw.delete(t[Ki])}var tw="__ngContext__";function _s(t,n){yr(n)?(t[tw]=n[Ki],gA(n)):t[tw]=n}function Xw(t){return eS(t[ds])}function Jw(t){return eS(t[Xn])}function eS(t){for(;t!==null&&!Jn(t);)t=t[Xn];return t}var Kg;function M_(t){Kg=t}function tS(){if(Kg!==void 0)return Kg;if(typeof document<"u")return document;throw new k(210,!1)}var nS="r";var iS="di";var rS=!1,oS=new w("",{factory:()=>rS});var nw=new WeakMap;function _A(t,n){if(t==null||typeof t!="object")return;let e=nw.get(t);e||(e=new WeakSet,nw.set(t,e)),e.add(n)}var vA=(t,n,e,i)=>{};function yA(t,n,e,i){vA(t,n,e,i)}function Uf(t){return(t.flags&32)===32}var bA=()=>null;function aS(t,n,e=!1){return bA(t,n,e)}function sS(t,n){let e=t.contentQueries;if(e!==null){let i=ce(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],a=e[r+1];if(a!==-1){let s=t.data[a];sl(o),s.contentQueries(2,n[a],a)}}}finally{ce(i)}}}function Zg(t,n,e){sl(0);let i=ce(null);try{n(t,e)}finally{ce(i)}}function cS(t,n,e){if(sg(n)){let i=ce(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let a=r;a<o;a++){let s=t.data[a];if(s.contentQueries){let c=e[a];s.contentQueries(1,c,a)}}}finally{ce(i)}}}var Di=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Di||{});var CA={"http://www.w3.org/2000/svg":os,"http://www.w3.org/1998/Math/MathML":Xu},gf;function wA(){if(gf===void 0&&(gf=null,Qn.trustedTypes))try{gf=Qn.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return gf}function Hf(t){return wA()?.createHTML(t)||t}var _f;function SA(){if(_f===void 0&&(_f=null,Qn.trustedTypes))try{_f=Qn.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return _f}function iw(t){return SA()?.createScriptURL(t)||t}var Sr=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${zu})`}},Qg=class extends Sr{getTypeName(){return"HTML"}},Xg=class extends Sr{getTypeName(){return"Style"}},Jg=class extends Sr{getTypeName(){return"Script"}},e_=class extends Sr{getTypeName(){return"URL"}},t_=class extends Sr{getTypeName(){return"ResourceURL"}};function ti(t){return t instanceof Sr?t.changingThisBreaksApplicationSecurity:t}function Dr(t,n){let e=lS(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${zu})`)}return e===n}function lS(t){return t instanceof Sr&&t.getTypeName()||null}function I_(t){return new Qg(t)}function N_(t){return new Xg(t)}function T_(t){return new Jg(t)}function k_(t){return new e_(t)}function A_(t){return new t_(t)}function DA(t){let n=new i_(t);return EA()?new n_(n):n}var n_=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Hf(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},i_=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Hf(n),e}};function EA(){try{return!!new window.DOMParser().parseFromString(Hf(""),"text/html")}catch{return!1}}var xA=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Sl(t){return t=String(t),t.match(xA)?t:"unsafe:"+t}function Er(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Dl(...t){let n={};for(let e of t)for(let i in e)Object.hasOwn(e,i)&&(n[i]=!0);return n}var dS=Er("area,br,col,hr,img,wbr"),uS=Er("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),fS=Er("rp,rt"),MA=Dl(fS,uS),IA=Dl(uS,Er("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),NA=Dl(fS,Er("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),rw=Dl(dS,IA,NA,MA),hS=Er("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),TA=Er("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),kA=Er("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),AA=Dl(hS,TA,kA),RA=Er("script,style,template"),r_=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=FA(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=PA(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=ow(n).toLowerCase();if(!Object.hasOwn(rw,e))return this.sanitizedSomething=!0,!Object.hasOwn(RA,e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),a=o.name,s=a.toLowerCase();if(!Object.hasOwn(AA,s)){this.sanitizedSomething=!0;continue}let c=o.value;hS[s]&&(c=Sl(c)),this.buf.push(" ",a,'="',aw(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=ow(n).toLowerCase();Object.hasOwn(rw,e)&&!Object.hasOwn(dS,e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(aw(n))}};function OA(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function PA(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw mS(n);return n}function FA(t){let n=t.firstChild;if(n&&OA(t,n))throw mS(n);return n}function ow(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function mS(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var LA=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,VA=/([^\#-~ |!])/g;function aw(t){return t.replace(/&/g,"&amp;").replace(LA,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(VA,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var vf;function R_(t,n){let e=null;try{vf=vf||DA(t);let i=n?String(n):"";e=vf.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=vf.getInertBodyElement(i)}while(i!==o);let s=new r_().sanitizeChildren(sw(e)||e);return Hf(s)}finally{if(e){let i=sw(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function sw(t){return"content"in t&&jA(t)?t.content:null}function jA(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}function BA(t,n){return t.createText(n)}function UA(t,n,e){t.setValue(n,e)}function pS(t,n,e){return t.createElement(n,e)}function da(t,n,e,i,r){t.insertBefore(n,e,i,r)}function gS(t,n,e){t.appendChild(n,e)}function cw(t,n,e,i,r){i!==null?da(t,n,e,i,r):gS(t,n,e)}function _S(t,n,e,i){t.removeChild(null,n,e,i)}function HA(t,n,e){t.setAttribute(n,"style",e)}function zA(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function vS(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&tA(t,n,i),r!==null&&zA(t,n,r),o!==null&&HA(t,n,o)}function $A(t,n=!0){if(t[0]!=":")return[null,t];let e=t.indexOf(":",1);if(e===-1){if(n)throw new Error(`Unsupported format "${t}" expecting ":namespace:name"`);return[null,t]}return[t.slice(1,e),t.slice(e+1)]}function tr(t){let n=bS();return n?n.sanitize(Oe.URL,t)||"":Dr(t,"URL")?ti(t):Sl(as(t))}function yS(t){let n=bS();if(n)return iw(n.sanitize(Oe.RESOURCE_URL,t)||"");if(Dr(t,"ResourceURL"))return iw(ti(t));throw new k(904,!1)}function GA(t,n){switch(WA(t,n)){case Oe.RESOURCE_URL:return yS;case Oe.URL:return tr;default:return null}}function O_(t,n,e){return GA(n,e)?.(t)??t}function bS(){let t=le();return t&&t[yi].sanitizer}function WA(t,n){let[e,i]=qA(t);return CC(i,n,e)}function qA(t){t=t.toLowerCase();let n=$A(t,!1);if(n[0])return n;let i=bi()===-1?null:sa(),r=i?.namespace;if(t==="#host"&&i?.type===2){let o=Pn(i,le());if(o.tagName&&(t=o.tagName.toLowerCase()),r==null){let a=o.namespaceURI;r=a&&CA[a]}}return[r,t]}function YA(t){return t instanceof Function?t():t}function KA(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var CS="ng-template";function ZA(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&KA(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(P_(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function P_(t){return t.type===4&&t.value!==CS}function QA(t,n,e){let i=t.type===4&&!e?CS:t.value;return n===i}function XA(t,n,e){let i=4,r=t.attrs,o=r!==null?tR(r):0,a=!1;for(let s=0;s<n.length;s++){let c=n[s];if(typeof c=="number"){if(!a&&!Ci(i)&&!Ci(c))return!1;if(a&&Ci(c))continue;a=!1,i=c|i&1;continue}if(!a)if(i&4){if(i=2|i&1,c!==""&&!QA(t,c,e)||c===""&&n.length===1){if(Ci(i))return!1;a=!0}}else if(i&8){if(r===null||!ZA(t,r,c,e)){if(Ci(i))return!1;a=!0}}else{let l=n[++s],d=JA(c,r,P_(t),e);if(d===-1){if(Ci(i))return!1;a=!0;continue}if(l!==""){let f;if(d>o?f="":f=r[d+1].toLowerCase(),i&2&&l!==f){if(Ci(i))return!1;a=!0}}}}return Ci(i)||a}function Ci(t){return(t&1)===0}function JA(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let a=n[r];if(a===t)return r;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++r];for(;typeof s=="string";)s=n[++r];continue}else{if(a===4)break;if(a===0){r+=4;continue}}r+=o?1:2}return-1}else return nR(n,t)}function wS(t,n,e=!1){for(let i=0;i<n.length;i++)if(XA(t,n[i],e))return!0;return!1}function eR(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function tR(t){for(let n=0;n<t.length;n++){let e=t[n];if(jw(e))return n}return t.length}function nR(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function iR(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function lw(t,n){return t?":not("+n.trim()+")":n}function rR(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(i&2){let s=t[++e];r+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else i&8?r+="."+a:i&4&&(r+=" "+a);else r!==""&&!Ci(a)&&(n+=lw(o,r),r=""),i=a,o=o||!Ci(i);e++}return r!==""&&(n+=lw(o,r)),n}function oR(t){return t.map(rR).join(",")}function aR(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!Ci(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var fn={},Ji=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Ji||{}),sR;function F_(t,n){return sR(t,n)}var uo=new Set;var N9=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var o_=new WeakMap;function SS(t){return t?t[ro]??t:null}var fl=new WeakSet;function cR(t,n,e){let i=o_.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,a=SS(e);for(let s=i.length-1;s>=0;s--){let{el:c,declarationView:l}=i[s],d=c.parentNode;c===n?(i.splice(s,1),fl.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&c===o?(i.splice(s,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):d&&r&&d!==r&&(a===null||l===null||a===l)&&(i.splice(s,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function lR(t,n,e){let i=SS(e),r=o_.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):o_.set(t,[{el:n,declarationView:i}])}var zf=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(zf||{}),nr=new w(""),dw=new Set;function xr(t){dw.has(t)||(dw.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var $f=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=me({token:t,providedIn:"root",factory:()=>new t})}return t})(),L_=[0,1,2,3],V_=(()=>{class t{ngZone=u(H);scheduler=u($i);errorHandler=u(ln,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u(nr,{optional:!0})}execute(){let e=this.sequences.size>0;e&&ze(Ae.AfterRenderHooksStart),this.executing=!0;for(let i of L_)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&ze(Ae.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[na]??=[]).push(e),aa(i),i[fe]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(zf.AFTER_NEXT_RENDER,e):e()}static \u0275prov=me({token:t,providedIn:"root",factory:()=>new t})}return t})(),gl=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,a=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[na];n&&(this.view[na]=n.filter(e=>e!==this))}};function mt(t,n){let e=n?.injector??u(te);return xr("NgAfterNextRender"),uR(t,e,n,!0)}function dR(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function uR(t,n,e,i){let r=n.get($f);r.impl??=n.get(V_);let o=n.get(nr,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(Et):null,s=n.get(hs,null,{optional:!0}),c=new gl(r.impl,dR(t),s?.view,i,a,o?.snapshot(null));return r.impl.register(c),c}var j_=new w("",{factory:()=>{let t=u(qe),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function DS(t,n,e){let i=t.get(j_);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function fR(t,n){let e=t.get(j_);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function hR(t,n){let e=t.get(j_);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function mR(t,n){for(let[e,i]of n)DS(t,i.animateFns)}function uw(t,n,e,i){let r=t?.[Zi]?.enter;n!==null&&r&&r.has(e.index)&&mR(i,r)}function fw(t,n,e,i){try{e.get(nl)}catch{return i(!1)}let r=t?.[Zi];r?.enter?.has(n.index)&&fR(e,r.enter.get(n.index).animateFns);let o=pR(t,n,r);if(o.size===0){let a=!1;if(t){let s=[];Gf(t,n,s),a=s.length>0}if(!a)return i(!1)}t&&uo.add(t[Ki]),DS(e,()=>gR(t,n,r||void 0,o,i),r||void 0)}function pR(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,a]of r){if(i.has(o))continue;let c=t[oe].data[o].parent;for(;c;){if(c===n){i.set(o,a);break}c=c.parent}}return i}function gR(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[a]of i){if(!e.leave.has(a))continue;let s=e.leave.get(a);for(let c of s.animateFns){let{promise:l}=c();o.push(l)}e.detachedLeaveAnimationFns=void 0}if(t&&Gf(t,n,o),o.length>0){let a=e||t?.[Zi];if(a){let s=a.running;s&&o.push(s),a.running=Promise.allSettled(o),vR(t,a.running,r)}else Promise.allSettled(o).then(()=>{t&&uo.delete(t[Ki]),r(!0)})}else t&&uo.delete(t[Ki]),r(!1)}function Gf(t,n,e){if(n.type&12){let r=t[n.index];if(Jn(r))for(let o=ft;o<r.length;o++){let a=r[o];a[oe].type===2&&_R(a,e)}}let i=n.child;for(;i;)Gf(t,i,e),i=i.next}function _R(t,n){let e=t[Zi];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:a}=o();n.push(a)}let i=t[oe].firstChild;for(;i;)Gf(t,i,n),i=i.next}function vR(t,n,e){n.then(()=>{t[Zi]?.running===n&&(t[Zi].running=void 0,uo.delete(t[Ki])),e(!0)})}function ms(t,n,e,i,r,o,a,s){if(r!=null){let c,l=!1;Jn(r)?c=r:yr(r)&&(l=!0,r=r[vi]);let d=Gt(r);t===0&&i!==null?(uw(s,i,o,e),a==null?gS(n,i,d):da(n,i,d,a||null,!0)):t===1&&i!==null?(uw(s,i,o,e),da(n,i,d,a||null,!0),cR(o,d,s)):t===2?(s?.[Zi]?.leave?.has(o.index)&&lR(o,d,s),fl.delete(d),fw(s,o,e,f=>{if(fl.has(d)){fl.delete(d);return}_S(n,d,l,f)})):t===3&&(fl.delete(d),fw(s,o,e,()=>{n.destroyNode(d)})),c!=null&&NR(n,t,e,c,o,i,a)}}function yR(t,n){ES(t,n),n[vi]=null,n[Jt]=null}function bR(t,n,e,i,r,o){i[vi]=r,i[Jt]=n,qf(t,i,e,1,r,o)}function ES(t,n){n[yi].changeDetectionScheduler?.notify(9),qf(t,n,n[Xe],2,null,null)}function CR(t){let n=t[ds];if(!n)return Pg(t[oe],t);for(;n;){let e=null;if(yr(n))e=n[ds];else{let i=n[ft];i&&(e=i)}if(!e){for(;n&&!n[Xn]&&n!==t;)yr(n)&&Pg(n[oe],n),n=n[Pt];n===null&&(n=t),yr(n)&&Pg(n[oe],n),e=n&&n[Xn]}n=e}}function B_(t,n){let e=t[ra],i=e.indexOf(n);e.splice(i,1)}function Wf(t,n){if(oa(n))return;let e=n[Xe];e.destroyNode&&qf(t,n,e,3,null,null),CR(n)}function Pg(t,n){if(oa(n))return;let e=ce(null);try{n[fe]&=-129,n[fe]|=256,n[On]&&Zr(n[On]),SR(t,n),wR(t,n),n[oe].type===1&&n[Xe].destroy();let i=n[oo];if(i!==null&&Jn(n[Pt])){i!==n[Pt]&&B_(i,n);let r=n[Yi];r!==null&&r.detachView(t)}Yg(n)}finally{ce(e)}}function wR(t,n){let e=t.cleanup,i=n[ls];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?i[s]():i[-s].unsubscribe(),a+=2}else{let s=i[e[a+1]];e[a].call(s)}i!==null&&(n[ls]=null);let r=n[gr];if(r!==null){n[gr]=null;for(let a=0;a<r.length;a++){let s=r[a];s()}}let o=n[_r];if(o!==null){n[_r]=null;for(let a of o)a.destroy()}}function SR(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof fa)){let o=e[i+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=r[o[a]],c=o[a+1];ze(Ae.LifecycleHookStart,s,c);try{c.call(s)}finally{ze(Ae.LifecycleHookEnd,s,c)}}else{ze(Ae.LifecycleHookStart,r,o);try{o.call(r)}finally{ze(Ae.LifecycleHookEnd,r,o)}}}}}function xS(t,n,e){return DR(t,n.parent,e)}function DR(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[vi];if(br(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Di.None||r===Di.Emulated)return null}return Pn(i,e)}function MS(t,n,e){return xR(t,n,e)}function ER(t,n,e){return t.type&40?Pn(t,e):null}var xR=ER,hw;function U_(t,n,e,i){let r=xS(t,i,n),o=n[Xe],a=i.parent||n[Jt],s=MS(a,i,n);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)cw(o,r,e[c],s,!1);else cw(o,r,e,s,!1);hw!==void 0&&hw(o,i,n,e,r)}function hl(t,n){if(n!==null){let e=n.type;if(e&3)return Pn(n,t);if(e&4)return a_(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return hl(t,i);{let r=t[n.index];return Jn(r)?a_(-1,r):Gt(r)}}else{if(e&128)return hl(t,n.next);if(e&32)return F_(n,t)()||Gt(t[n.index]);{let i=IS(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=vr(t[dn]);return hl(r,i)}else return hl(t,n.next)}}}return null}function IS(t,n){if(n!==null){let i=t[dn][Jt],r=n.projection;return i.projection[r]}return null}function a_(t,n){let e=ft+t+1;if(e<n.length){let i=n[e],r=i[oe].firstChild;if(r!==null)return hl(i,r)}return n[ia]}function H_(t,n,e,i,r,o,a){for(;e!=null;){let s=i[qi];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(a&&n===0&&(c&&_s(Gt(c),i),e.flags|=2),!Uf(e))if(l&8)H_(t,n,e.child,i,r,o,!1),ms(n,t,s,r,c,e,o,i);else if(l&32){let d=F_(e,i),f;for(;f=d();)ms(n,t,s,r,f,e,o,i);ms(n,t,s,r,c,e,o,i)}else l&16?NS(t,n,i,e,r,o):ms(n,t,s,r,c,e,o,i);e=a?e.projectionNext:e.next}}function qf(t,n,e,i,r,o){t.type===3?MR(e,i,n,r,o):H_(e,i,t.firstChild,n,r,o,!1)}function MR(t,n,e,i,r){let a=e[oe].firstChild,s=a.next,c=Gt(e[a.index]),l=Gt(e[s.index]),d=s.index+1,f=e[d];if(n===1||n===0)i!==null&&(f&&f.hasChildNodes()?da(t,i,f,r,!0):(da(t,i,c,r,!0),da(t,i,l,r,!0)));else if(n===2){if(f||(f=document.createDocumentFragment(),e[d]=f),c&&c.parentNode===f)return;let p=c;for(;p!==null;){let _=p.nextSibling;if(f.appendChild(p),p===l)break;p=_}}}function IR(t,n,e){let i=n[Xe],r=xS(t,e,n),o=e.parent||n[Jt],a=MS(o,e,n);NS(i,0,n,e,r,a)}function NS(t,n,e,i,r,o){let a=e[dn],c=a[Jt].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];ms(n,t,e[qi],r,d,i,o,e)}else{let l=c,d=a[Pt];Zw(i)&&(l.flags|=128),H_(t,n,l,d,r,o,!0)}}function NR(t,n,e,i,r,o,a){let s=i[ia],c=Gt(i);if(s!==c&&ms(n,t,e,o,s,r,a),(i[fe]&4)===0)for(let l=ft;l<i.length;l++){let d=i[l];qf(d[oe],d,t,n,o,s)}}function TR(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Ji.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Ji.Important),t.setStyle(e,i,r,o))}}function z_(t,n,e,i,r,o,a,s,c,l,d){let f=ut+i,p=f+r,_=kR(f,p),b=typeof l=="function"?l():l;return _[oe]={type:t,blueprint:_,template:e,queries:null,viewQuery:s,declTNode:n,data:_.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:p,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:c,consts:b,incompleteFirstPass:!1,ssrId:d}}function kR(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:fn);return e}function AR(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=z_(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function $_(t,n,e,i,r,o,a,s,c,l,d){let f=n.blueprint.slice();return f[vi]=r,f[fe]=i|4|128|8|64|1024,(l!==null||t&&t[fe]&2048)&&(f[fe]|=2048),ug(f),f[Pt]=f[ro]=t,f[yt]=e,f[yi]=a||t&&t[yi],f[Xe]=s||t&&t[Xe],f[qi]=c||t&&t[qi]||null,f[Jt]=o,f[Ki]=pA(),f[ea]=d,f[og]=l,f[dn]=n.type==2?t[dn]:f,f}function RR(t,n,e){let i=Pn(n,t),r=AR(e),o=t[yi].rendererFactory,a=G_(t,$_(t,r,null,TS(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=a}function TS(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function kS(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function G_(t,n){return t[ds]?t[rg][Xn]=n:t[ds]=n,t[rg]=n,n}function g(t=1){AS(tt(),le(),bi()+t,!1)}function AS(t,n,e,i){if(!i)if((n[fe]&3)===3){let o=t.preOrderCheckHooks;o!==null&&bf(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Cf(n,o,0,e)}so(e)}var Yf=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Yf||{});function ha(t,n,e,i){let r=ce(null);try{let[o,a,s]=t.inputs[e],c=null;(a&Yf.SignalBased)!==0&&(c=n[o][_t]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,o):Ow(n,c,o,i)}finally{ce(r)}}function RS(t,n,e,i,r){let o=bi(),a=i&2;try{so(-1),a&&n.length>ut&&AS(t,n,ut,!1);let s=a?Ae.TemplateUpdateStart:Ae.TemplateCreateStart;ze(s,r,e),e(i,r)}finally{so(o);let s=a?Ae.TemplateUpdateEnd:Ae.TemplateCreateEnd;ze(s,r,e)}}function W_(t,n,e){jR(t,n,e),(e.flags&64)===64&&BR(t,n,e)}function Kf(t,n,e=Pn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let a=i[o+1],s=a===-1?e(n,t):t[a];t[r++]=s}}}function OR(t,n,e,i){let o=i.get(oS,rS)||e===Di.ShadowDom||e===Di.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);return PR(a),a}function PR(t){FR(t)}var FR=()=>null;function LR(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function OS(t,n,e,i,r,o){let a=n[oe];if(q_(t,a,n,e,i)){br(t)&&VR(n,t.index);return}t.type&3&&(e=LR(e)),PS(t,n,e,i,r,o)}function PS(t,n,e,i,r,o){if(t.type&3){let a=Pn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(a,e,i)}else t.type&12}function VR(t,n){let e=ei(n,t);e[fe]&16||(e[fe]|=64)}function jR(t,n,e){let i=e.directiveStart,r=e.directiveEnd;br(e)&&RR(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||If(e,n);let o=e.initialInputs;for(let a=i;a<r;a++){let s=t.data[a],c=pl(n,t,a,e);if(_s(c,n),o!==null&&$R(n,a-i,c,s,e,o),Qi(s)){let l=ei(e.index,n);l[yt]=pl(n,t,a,e)}}}function BR(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,a=PC();try{so(o);for(let s=i;s<r;s++){let c=t.data[s],l=n[s];af(s),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&UR(c,l)}}finally{so(-1),af(a)}}function UR(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function FS(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];wS(n,o.selectors,!1)&&(i??=[],Qi(o)?i.unshift(o):i.push(o))}return i}function HR(t,n,e,i,r,o){let a=Pn(t,n);zR(n[Xe],a,o,t.value,e,i,r)}function zR(t,n,e,i,r,o,a){if(o==null)a?.(o,i||"",r),t.removeAttribute(n,r,e);else{let s=a==null?as(o):a(o,i||"",r);t.setAttribute(n,r,s,e)}}function $R(t,n,e,i,r,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let c=a[s],l=a[s+1];ha(i,e,c,l)}}function LS(t,n,e,i,r){let o=ut+e,a=n[oe],s=r(a,n,t,i,e);n[o]=s,fs(t,!0);let c=t.type===2;return c?(vS(n[Xe],s,t),(MC()===0||ol(t))&&_s(s,n),IC()):_s(s,n),df()&&(!c||!Uf(t))&&U_(a,n,s,t),t}function VS(t){let n=t;return Cg()?wg():(n=n.parent,fs(n,!1)),n}function GR(t,n){let e=t[qi];if(!e)return;let i;try{i=e.get(Ln,null)}catch{i=null}i?.(n)}function q_(t,n,e,i,r){let o=t.inputs?.[i],a=t.hostDirectiveInputs?.[i],s=!1;if(a)for(let c=0;c<a.length;c+=2){let l=a[c],d=a[c+1],f=n.data[l];ha(f,e[l],d,r),s=!0}if(o)for(let c of o){let l=e[c],d=n.data[c];ha(d,l,i,r),s=!0}return s}function WR(t,n,e,i,r,o){let a=null,s=null,c=null,l=!1,d=t.directiveToIndex.get(i.type);if(typeof d=="number"?a=d:[a,s,c]=d,s!==null&&c!==null&&t.hostDirectiveInputs&&Object.hasOwn(t.hostDirectiveInputs,r)){let f=t.hostDirectiveInputs[r];for(let p=0;p<f.length;p+=2){let _=f[p];if(_>=s&&_<=c){let b=n.data[_],M=f[p+1];ha(b,e[_],M,o),l=!0}else if(_>c)break}}return a!==null&&Object.hasOwn(i.inputs,r)&&(ha(i,e[a],r,o),l=!0),l}function qR(t,n){let e=ei(n,t),i=e[oe];YR(i,e);let r=e[vi];r!==null&&e[ea]===null&&(e[ea]=aS(r,e[qi])),ze(Ae.ComponentStart);try{Y_(i,e,e[yt])}finally{ze(Ae.ComponentEnd,e[yt])}}function YR(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function Y_(t,n,e){cf(n);try{let i=t.viewQuery;i!==null&&Zg(1,i,e);let r=t.template;r!==null&&RS(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Yi]?.finishViewCreation(t),t.staticContentQueries&&sS(t,n),t.staticViewQueries&&Zg(2,t.viewQuery,e);let o=t.components;o!==null&&KR(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[fe]&=-5,lf()}}function KR(t,n){for(let e=0;e<n.length;e++)qR(t,n[e])}function El(t,n,e,i){let r=ce(null);try{let o=n.tView,s=t[fe]&4096?4096:16,c=$_(t,o,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[oo]=l;let d=t[Yi];return d!==null&&(c[Yi]=d.createEmbeddedView(o)),Y_(o,c,e),c}finally{ce(r)}}function vs(t,n){return!n||n.firstChild===null||Zw(t)}function _l(t,n,e,i,r=!1){if(t.type===3){let o=t.firstChild,a=o.next,s=Gt(n[o.index]),c=Gt(n[a.index]),l=s;for(;l!==null&&(i.push(l),l!==c);)l=l.nextSibling;return i}for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];if(o!==null)if(Jn(o)){let s=o[ia];s!==o[vi]&&i.push(Gt(o)),o[fe]&4||jS(o,i),i.push(s)}else i.push(Gt(o));let a=e.type;if(a&8)_l(t,n,e.child,i);else if(a&32){let s=F_(e,n),c;for(;c=s();)i.push(c)}else if(a&16){let s=IS(n,e);if(Array.isArray(s))i.push(...s);else{let c=vr(n[dn]);_l(c[oe],c,s,i,!0)}}e=r?e.projectionNext:e.next}return i}function jS(t,n){for(let e=ft;e<t.length;e++){let i=t[e],r=i[oe].firstChild;r!==null&&_l(i[oe],i,r,n)}}function BS(t){if(t[na]!==null){for(let n of t[na])n.impl.addSequence(n);t[na].length=0}}var US=[];function ZR(t){return t[On]??QR(t)}function QR(t){let n=US.pop()??Object.create(JR);return n.lView=t,n}function XR(t){t.lView[On]!==t&&(t.lView=null,US.push(t))}var JR=re(C({},qr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{aa(t.lView)},consumerOnSignalRead(){this.lView[On]=this}});function eO(t){let n=t[On]??Object.create(tO);return n.lView=t,n}var tO=re(C({},qr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=vr(t.lView);for(;n&&!HS(n[oe]);)n=vr(n);n&&fg(n)},consumerOnSignalRead(){this.lView[On]=this}});function HS(t){return t.type!==2}function zS(t){if(t[_r]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[_r])if(i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()),t[_r]===null))return;n=e&&!!(t[fe]&8192)}}var nO=100;function $S(t,n=0){let i=t[yi].rendererFactory,r=!1;r||i.begin?.();try{iO(t,n)}finally{r||i.end?.()}}function iO(t,n){let e=Sg();try{Yc(!0),s_(t,n);let i=0;for(;al(t);){if(i===nO)throw new k(103,!1);i++,s_(t,1)}}finally{Yc(e)}}function rO(t,n,e,i){if(oa(n))return;let r=n[fe],o=!1,a=!1;cf(n);let s=!0,c=null,l=null;o||(HS(t)?(l=ZR(n),c=hr(l)):nu()===null?(s=!1,l=eO(n),c=hr(l)):n[On]&&(Zr(n[On]),n[On]=null));try{ug(n),AC(t.bindingStartIndex),e!==null&&RS(t,n,e,2,i);let d=(r&3)===3;if(!o)if(d){let _=t.preOrderCheckHooks;_!==null&&bf(n,_,null)}else{let _=t.preOrderHooks;_!==null&&Cf(n,_,0,null),Rg(n,0)}if(a||oO(n),zS(n),GS(n,0),t.contentQueries!==null&&sS(t,n),!o)if(d){let _=t.contentCheckHooks;_!==null&&bf(n,_)}else{let _=t.contentHooks;_!==null&&Cf(n,_,1),Rg(n,1)}sO(t,n);let f=t.components;f!==null&&qS(n,f,0);let p=t.viewQuery;if(p!==null&&Zg(2,p,i),!o)if(d){let _=t.viewCheckHooks;_!==null&&bf(n,_)}else{let _=t.viewHooks;_!==null&&Cf(n,_,2),Rg(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Qu]){for(let _ of n[Qu])_();n[Qu]=null}o||(BS(n),n[fe]&=-73)}catch(d){throw o||aa(n),d}finally{l!==null&&(Kr(l,c),s&&XR(l)),lf()}}function GS(t,n){for(let e=Xw(t);e!==null;e=Jw(e))for(let i=ft;i<e.length;i++){let r=e[i];WS(r,n)}}function oO(t){for(let n=Xw(t);n!==null;n=Jw(n)){if(!(n[fe]&2))continue;let e=n[ra];for(let i=0;i<e.length;i++){let r=e[i];fg(r)}}}function aO(t,n,e){ze(Ae.ComponentStart);let i=ei(n,t);try{WS(i,e)}finally{ze(Ae.ComponentEnd,i[yt])}}function WS(t,n){ef(t)&&s_(t,n)}function s_(t,n){let i=t[oe],r=t[fe],o=t[On],a=!!(n===0&&r&16);if(a||=!!(r&64&&n===0),a||=!!(r&1024),a||=!!(o?.dirty&&Za(o)),a||=!1,o&&(o.dirty=!1),t[fe]&=-9217,a)rO(i,t,i.template,t[yt]);else if(r&8192){let s=ce(null);try{zS(t),GS(t,1);let c=i.components;c!==null&&qS(t,c,1),BS(t)}finally{ce(s)}}}function qS(t,n,e){for(let i=0;i<n.length;i++)aO(t,n[i],e)}function sO(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)so(~r);else{let o=r,a=e[++i],s=e[++i];OC(a,o);let c=n[o];ze(Ae.HostBindingsUpdateStart,c);try{s(2,c)}finally{ze(Ae.HostBindingsUpdateEnd,c)}}}}finally{so(-1)}}function K_(t,n){let e=Sg()?64:1088;for(t[yi].changeDetectionScheduler?.notify(n);t;){t[fe]|=e;let i=vr(t);if(us(t)&&!i)return t;t=i}return null}function YS(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function KS(t,n){let e=ft+n;if(e<t.length)return t[e]}function xl(t,n,e,i=!0){let r=n[oe];if(cO(r,n,t,e),i){let a=a_(e,t),s=n[Xe],c=s.parentNode(t[ia]);c!==null&&bR(r,t[Jt],s,n,c,a)}let o=n[ea];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function ZS(t,n){let e=vl(t,n);return e!==void 0&&Wf(e[oe],e),e}function vl(t,n){if(t.length<=ft)return;let e=ft+n,i=t[e];if(i){let r=i[oo];r!==null&&r!==t&&B_(r,i),n>0&&(t[e-1][Xn]=i[Xn]);let o=tl(t,ft+n);yR(i[oe],i);let a=o[Yi];a!==null&&a.detachView(o[oe]),i[Pt]=null,i[Xn]=null,i[fe]&=-129}return i}function cO(t,n,e,i){let r=ft+i,o=e.length;i>0&&(e[r-1][Xn]=n),i<o-ft?(n[Xn]=e[r],Jp(e,ft+i,n)):(e.push(n),n[Xn]=null),n[Pt]=e;let a=n[oo];a!==null&&e!==a&&QS(a,n);let s=n[Yi];s!==null&&s.insertView(t),tf(n),n[fe]|=128}function QS(t,n){let e=t[ra],i=n[Pt];if(yr(i))t[fe]|=2;else{let r=i[Pt][dn];n[dn]!==r&&(t[fe]|=2)}e===null?t[ra]=[n]:e.push(n)}var fo=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[oe];return _l(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[yt]}set context(n){this._lView[yt]=n}get destroyed(){return oa(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Pt];if(Jn(n)){let e=n[rl],i=e?e.indexOf(this):-1;i>-1&&(vl(n,i),tl(e,i))}this._attachedToViewContainer=!1}Wf(this._lView[oe],this._lView)}onDestroy(n){nf(this._lView,n)}markForCheck(){K_(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[fe]&=-129}reattach(){tf(this._lView),this._lView[fe]|=128}detectChanges(){this._lView[fe]|=1024,$S(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new k(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=us(this._lView),e=this._lView[oo];e!==null&&!n&&B_(e,this._lView),ES(this._lView[oe],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new k(902,!1);this._appRef=n;let e=us(this._lView),i=this._lView[oo];i!==null&&!e&&QS(i,this._lView),tf(this._lView)}};var un=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=lO;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=El(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new fo(o)}}return t})();function lO(){return Zf(xt(),le())}function Zf(t,n){return t.type&4?new un(n,t,ws(t,n)):null}function Ss(t,n,e,i,r){let o=t.data[n];if(o===null)o=dO(t,n,e,i,r),RC()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let a=NC();o.injectorIndex=a===null?-1:a.injectorIndex}return fs(o,!0),o}function dO(t,n,e,i,r){let o=bg(),a=Cg(),s=a?o:o&&o.parent,c=t.data[n]=fO(t,s,e,n,i,r);return uO(t,c,o,a),c}function uO(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function fO(t,n,e,i,r,o){let a=n?n.injectorIndex:-1,s=0;return _g()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:r,namespace:xg(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function hO(t){let n=t[ag]??[],i=t[Pt][Xe],r=[];for(let o of n)o.data[iS]!==void 0?r.push(o):mO(o,i);t[ag]=r}function mO(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[nS];for(;e<r;){let o=i.nextSibling;_S(n,i,!1),i=o,e++}}}var pO=()=>null,gO=()=>null;function Nf(t,n){return pO(t,n)}function XS(t,n,e){return gO(t,n,e)}var JS=class{},Mt=class{},Te=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>_O()};function _O(){let t=le(),n=xt(),e=ei(n.index,t);return(yr(e)?e:t)[Xe]}var eD=(()=>{class t{static \u0275prov=me({token:t,providedIn:"root",factory:()=>null})}return t})();function tD(t){return t.debugInfo?.className||t.type.name||null}var Sf={},Tf=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Sf,i);return r!==Sf||e===Sf?r:this.parentInjector.get(n,e,i)}};function Z_(t,n,e){return t[n]=e}function vO(t,n){return t[n]}function ni(t,n,e){if(e===fn)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function nD(t,n,e,i){let r=ni(t,n,e);return ni(t,n+1,i)||r}function ua(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&_A(r,o);let a=br(t)?ei(t.index,n):n;K_(a,5);let s=n[yt],c=mw(n,s,e,r),l=i.__ngNextListenerFn__;for(;l;)c=mw(n,s,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function mw(t,n,e,i){let r=ce(null);try{return ze(Ae.OutputStart,n,e),e(i)!==!1}catch(o){return GR(t,o),!1}finally{ze(Ae.OutputEnd,n,e),ce(r)}}function Q_(t,n,e,i,r,o,a,s){let c=ol(t),l=!1,d=null;if(!i&&c&&(d=bO(n,e,o,t.index)),d!==null){let f=d.__ngLastListenerFn__||d;f.__ngNextListenerFn__=a,d.__ngLastListenerFn__=a,l=!0}else{let f=Pn(t,e),p=i?i(f):f;yA(e,p,o,s),i||(s.__ngNativeEl__=f);let _=r.listen(p,o,s);if(!yO(o)){let b=i?M=>i(Gt(M[t.index])):t.index;iD(b,n,e,o,s,_,!1)}}return l}function yO(t){return t.startsWith("animation")||t.startsWith("transition")}function bO(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let a=r[o];if(a===e&&r[o+1]===i){let s=n[ls],c=r[o+2];return s&&s.length>c?s[c]:null}typeof a=="string"&&(o+=2)}return null}function iD(t,n,e,i,r,o,a){let s=n.firstCreatePass?mg(n):null,c=hg(e),l=c.length;c.push(r,o),s&&s.push(i,t,l,(l+1)*(a?-1:1))}function pw(t,n,e,i,r){let o=null,a=null,s=null,c=!1,l=t.directiveToIndex.get(e.type);if(typeof l=="number"?o=l:[o,a,s]=l,a!==null&&s!==null&&t.hostDirectiveOutputs&&Object.hasOwn(t.hostDirectiveOutputs,i)){let d=t.hostDirectiveOutputs[i];for(let f=0;f<d.length;f+=2){let p=d[f];if(p>=a&&p<=s)c=!0,kf(t,n,p,d[f+1],i,r);else if(p>s)break}}return Object.hasOwn(e.outputs,i)&&(c=!0,kf(t,n,o,i,i,r)),c}function kf(t,n,e,i,r,o){let a=n[e],s=n[oe],l=s.data[e].outputs[i],f=a[l].subscribe(o);iD(t.index,s,n,r,o,f,!0)}function Fe(){CO()}function CO(){let t=le(),n=tt(),e=xt();if(n.firstCreatePass&&SO(n,e),e.controlDirectiveIndex===-1)return;xr("NgSignalForms");let i=t[e.controlDirectiveIndex];n.data[e.controlDirectiveIndex].controlDef.create(i,new Af(t,n,e))}function Le(){wO()}function wO(){let t=le(),n=tt(),e=sa();if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef,r=t[e.controlDirectiveIndex];i.update(r,new Af(t,n,e))}var Af=class{lView;tView;tNode;hasPassThrough;constructor(n,e,i){this.lView=n,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return Pn(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,e){let i=this.tView.data[this.tNode.customControlIndex];pw(this.tNode,this.lView,i,n,ua(this.tNode,this.lView,e))}listenToCustomControlModel(n){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];pw(this.tNode,this.lView,i,e,ua(this.tNode,this.lView,n))}listenToDom(n,e){Q_(this.tNode,this.tView,this.lView,void 0,this.lView[Xe],n,e,ua(this.tNode,this.lView,e))}setInputOnDirectives(n,e){let i=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!i&&!r)return!1;let o=!1;if(i)for(let a of i){if(a===this.tNode.controlDirectiveIndex)continue;let s=this.tView.data[a],c=this.lView[a];ha(s,c,n,e),o=!0}if(r)for(let a=0;a<r.length;a+=2){let s=r[a];if(s===this.tNode.controlDirectiveIndex)continue;let c=r[a+1],l=this.tView.data[s],d=this.lView[s];ha(l,d,c,e),o=!0}return o}setCustomControlModelInput(n){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";WR(this.tNode,this.tView,this.lView,e,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[n]===!0}_buildCustomControlInputCache(n){let e={};for(let i in n.inputs)e[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let a in r.inputs)e[r.inputs[a]]=!0;let o=gw(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let s=0;s<o.inputs.length;s+=2){let c=o.inputs[s+1]||o.inputs[s];e[c]=!0}let a=gw(o.directive);a!==null&&i.push(...a)}}}return e}};function gw(t){return typeof t=="function"&&"\u0275dir"in t?t.\u0275dir.hostDirectives??null:null}function SO(t,n,e){for(let r=n.directiveStart;r<n.directiveEnd;r++)if(t.data[r].controlDef){n.controlDirectiveIndex=r;break}if(n.controlDirectiveIndex===-1)return;let i=t.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}DO(t,n)}function DO(t,n){for(let e=n.directiveStart;e<n.directiveEnd;e++){let i=t.data[e];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(_w(i,"value")){n.flags|=1024,n.customControlIndex=e;return}if(_w(i,"checked")){n.flags|=2048,n.customControlIndex=e;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let e=(i,r)=>{let o=n.hostDirectiveInputs[i],a=n.hostDirectiveOutputs[i+"Change"];if(!o||!a)return!1;for(let s=0;s<o.length;s+=2){let c=o[s];for(let l=0;l<a.length;l+=2){let d=a[l];if(c===d)for(let f of n.directiveToIndex.values()){if(!Array.isArray(f))continue;let[p,_,b]=f;if(c>=_&&c<=b)return n.flags|=r,n.customControlIndex=p,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function _w(t,n){return EO(t,n)&&xO(t,n+"Change")}function EO(t,n){return n in t.inputs}function xO(t,n){return n in t.outputs}var c_=Symbol("BINDING");var pa=new w("");function Rf(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)r=$u(r,s);else if(o==2){let c=s,l=n[++a];i=$u(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ie(t,n=0){let e=le();if(e===null)return J(t,n);let i=xt();return Ww(i,e,$t(t),n)}function ga(){let t="invalid";throw new Error(t)}function rD(t,n,e,i,r){let o=i===null?null:{"":-1},a=r(t,e);if(a!==null){let s=a,c=null,l=null;for(let d of a)if(d.resolveHostDirectives!==null){[s,c,l]=d.resolveHostDirectives(a);break}NO(t,n,e,s,o,c,l)}o!==null&&i!==null&&MO(e,i,o)}function MO(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new k(-301,!1);i.push(n[r],o)}}function IO(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function NO(t,n,e,i,r,o,a){let s=i.length,c=null;for(let p=0;p<s;p++){let _=i[p];c===null&&Qi(_)&&(c=_,IO(t,e,p)),Wg(If(e,n),t,_.type)}PO(e,t.data.length,s),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let p=0;p<s;p++){let _=i[p];_.providersResolver&&_.providersResolver(_)}let l=!1,d=!1,f=kS(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let p=0;p<s;p++){let _=i[p];if(e.mergedAttrs=gs(e.mergedAttrs,_.hostAttrs),kO(t,e,n,f,_),OO(f,_,r),a!==null&&a.has(_)){let[M,x]=a.get(_);e.directiveToIndex.set(_.type,[f,M+e.directiveStart,x+e.directiveStart])}else(o===null||!o.has(_))&&e.directiveToIndex.set(_.type,f);_.contentQueries!==null&&(e.flags|=4),(_.hostBindings!==null||_.hostAttrs!==null||_.hostVars!==0)&&(e.flags|=64);let b=_.type.prototype;!l&&(b.ngOnChanges||b.ngOnInit||b.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!d&&(b.ngOnChanges||b.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),f++}TO(t,e,o)}function TO(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))vw(0,n,r,i),vw(1,n,r,i),bw(n,i,!1);else{let o=e.get(r);yw(0,n,o,i),yw(1,n,o,i),bw(n,i,!0)}}}function vw(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(i),oD(n,o)}}function yw(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(Object.hasOwn(r,o)){let a=r[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(i,o),oD(n,a)}}function oD(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function bw(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||P_(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<i.length;){let c=i[s];if(c===0){s+=4;continue}else if(c===5){s+=2;continue}else if(typeof c=="number")break;if(!e&&Object.hasOwn(r,c)){let l=r[c];for(let d of l)if(d===n){a??=[],a.push(c,i[s+1]);break}}else if(e&&Object.hasOwn(o,c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){a??=[],a.push(l[d+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function kO(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=to(r.type,!0)),a=new fa(o,Qi(r),ie,null);t.blueprint[i]=a,e[i]=a,AO(t,n,i,kS(t,e,r.hostVars,fn),r)}function AO(t,n,e,i,r){let o=r.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;RO(a)!=s&&a.push(s),a.push(e,i,o)}}function RO(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function OO(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Qi(n)&&(e[""]=t)}}function PO(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function aD(t,n,e,i,r,o,a,s){let c=n[oe],l=c.consts,d=Fn(l,a),f=Ss(c,t,e,i,d);return o&&rD(c,n,f,Fn(l,s),r),f.mergedAttrs=gs(f.mergedAttrs,f.attrs),f.attrs!==null&&Rf(f,f.attrs,!1),f.mergedAttrs!==null&&Rf(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function sD(t,n){Lw(t,n),sg(n)&&t.queries.elementEnd(n)}function FO(t,n,e,i,r,o){let a=n.consts,s=Fn(a,r),c=Ss(n,t,e,i,s);if(c.mergedAttrs=gs(c.mergedAttrs,c.attrs),o!=null){let l=Fn(a,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Rf(c,c.attrs,!1),c.mergedAttrs!==null&&Rf(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var cD=typeof ShadowRoot<"u",LO=typeof Document<"u";function VO(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Yf.SignalBased)!==0};return r&&(o.transform=r),o})}function jO(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function BO(t,n,e){let i=n instanceof qe?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Tf(e,i):e}function UO(t){let n=t.get(Mt,null);if(n===null)throw new k(407,!1);let e=t.get(eD,null),i=t.get($i,null),r=t.get(nr,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function HO(t,n){let e=lD(t);return pS(n,e,e==="svg"?os:e==="math"?Xu:null)}function zO(t){if((t&&"localName"in t&&typeof t.localName=="string"?t.localName:t?.tagName)?.toLowerCase()==="script")throw new k(905,!1)}function lD(t){return(t.selectors[0][0]||"div").toLowerCase()}var ys=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=VO(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=jO(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=oR(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,a){ze(Ae.DynamicComponentStart);let s=ce(null);try{let c=this.componentDef,l=BO(c,r||this.ngModule,n),d=UO(l),f=d.tracingService;return f&&f.componentCreate?f.componentCreate(tD(c),()=>this.createComponentRef(d,l,e,i,o,a)):this.createComponentRef(d,l,e,i,o,a)}finally{ce(s)}}createComponentRef(n,e,i,r,o,a){let s=this.componentDef,c=$O(r,s,a,o),l=n.rendererFactory.createRenderer(null,s),d=r?OR(l,r,s.encapsulation,e):HO(s,l);zO(d);let f=e.get(pa,null),p=GO(d,()=>e.get($,null)??tS());f&&f.addHost(p);let _=a?.some(Cw)||o?.some(x=>typeof x!="function"&&x.bindings.some(Cw)),b=$_(null,c,null,512|TS(s),null,null,n,l,e,null,aS(d,e,!0));f&&cD&&p instanceof ShadowRoot&&nf(b,()=>{f.removeHost(p)}),b[ut]=d,cf(b);let M=null;try{let x=aD(ut,b,2,"#host",()=>c.directiveRegistry,!0,0);vS(l,d,x),_s(d,b),W_(c,b,x),cS(c,x,b),sD(c,x),i!==void 0&&qO(x,this.ngContentSelectors,i),M=ei(x.index,b),b[yt]=M[yt],Y_(c,b,null)}catch(x){throw M!==null&&Yg(M),Yg(b),x}finally{ze(Ae.DynamicComponentEnd),lf()}return new Of(this.componentType,b,!!_)}};function $O(t,n,e,i){let r=t?["ng-version","22.1.3"]:aR(n.selectors[0]),o=null,a=null,s=0;if(e)for(let d of e)s+=d[c_].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(a??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let f=i[d];if(typeof f!="function")for(let p of f.bindings){s+=p[c_].requiredVars;let _=d+1;p.create&&(p.targetIdx=_,(o??=[]).push(p)),p.update&&(p.targetIdx=_,(a??=[]).push(p))}}let c=[n];if(i)for(let d of i){let f=typeof d=="function"?d:d.type,p=Zp(f);c.push(p)}return z_(0,null,WO(o,a),1,s,c,null,null,null,[r],null)}function GO(t,n){let e=t.getRootNode?.();return LO&&e instanceof Document?e.head:e&&cD&&e instanceof ShadowRoot?e:n().head}function WO(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function Cw(t){let n=t[c_].kind;return n==="input"||n==="twoWay"}var Of=class extends JS{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Ju(e[oe],ut),this.location=ws(this._tNode,e),this.instance=ei(this._tNode.index,e)[yt],this.hostView=this.changeDetectorRef=new fo(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=q_(i,r[oe],r,n,e);this.previousInputValues.set(n,e);let a=ei(i.index,r);K_(a,1)}get injector(){return new lo(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function qO(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Lt=(()=>{class t{static __NG_ELEMENT_ID__=YO}return t})();function YO(){let t=xt();return dD(t,le())}var l_=class t extends Lt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return ws(this._hostTNode,this._hostLView)}get injector(){return new lo(this._hostTNode,this._hostLView)}get parentInjector(){let n=E_(this._hostTNode,this._hostLView);if(Bw(n)){let e=xf(n,this._hostLView),i=Ef(n),r=e[oe].data[i+8];return new lo(r,e)}else return new lo(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=ww(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-ft}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let a=Nf(this._lContainer,n.ssrId),s=n.createEmbeddedViewImpl(e||{},o,a);return this.insertImpl(s,r,vs(this._hostTNode,a)),s}createComponent(n,e,i,r,o,a,s){let c,l=e||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,a=l.directives,s=l.bindings;let d=new ys(no(n)),f=i||this.parentInjector;if(!o&&d.ngModule==null){let E=this.parentInjector.get(qe,null);E&&(o=E)}let p=no(d.componentType??{}),_=Nf(this._lContainer,p?.id??null),b=_?.firstChild??null,M=d.create(f,r,b,o,a,s);return this.insertImpl(M.hostView,c,vs(this._hostTNode,_)),M}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(SC(r)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let c=r[Pt],l=new t(c,c[Jt],c[Pt]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return xl(a,r,o,i),n.attachToViewContainerRef(),Jp(Fg(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=ww(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=vl(this._lContainer,e);i&&(tl(Fg(this._lContainer),e),Wf(i[oe],i))}detach(n){let e=this._adjustIndex(n,-1),i=vl(this._lContainer,e);return i&&tl(Fg(this._lContainer),e)!=null?new fo(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function ww(t){return t[rl]}function Fg(t){return t[rl]||(t[rl]=[])}function dD(t,n){let e,i=n[t.index];return Jn(i)?e=i:(e=YS(i,n,null,t),n[t.index]=e,G_(n,e)),ZO(e,n,t,i),new l_(e,t,n)}function KO(t,n){let e=t[Xe],i=e.createComment(""),r=Pn(n,t),o=e.parentNode(r);return da(e,o,i,e.nextSibling(r),!1),i}var ZO=JO,QO=()=>!1;function XO(t,n,e){return QO(t,n,e)}function JO(t,n,e,i){if(t[ia])return;let r;e.type&8?r=Gt(i):r=KO(n,e),t[ia]=r}var d_=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},u_=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];r.push(s.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)J_(n,e).matches!==null&&this.queries[e].setDirty()}},Pf=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=r1(n):this.predicate=n}},f_=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},h_=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,e1(e,o)),this.matchTNodeWithReadOption(n,e,wf(e,n,o,!1,!1))}else i===un?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,wf(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===U||r===Lt||r===un&&e.type&4)this.addMatch(e.index,-2);else{let o=wf(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function e1(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function t1(t,n){return t.type&11?ws(t,n):t.type&4?Zf(t,n):null}function n1(t,n,e,i){return e===-1?t1(n,t):e===-2?i1(t,n,i):pl(t,t[oe],e,n)}function i1(t,n,e){if(e===U)return ws(n,t);if(e===un)return Zf(n,t);if(e===Lt)return dD(n,t)}function uD(t,n,e,i){let r=n[Yi].queries[i];if(r.matches===null){let o=t.data,a=e.matches,s=[];for(let c=0;a!==null&&c<a.length;c+=2){let l=a[c];if(l<0)s.push(null);else{let d=o[l];s.push(n1(n,d,a[c+1],e.metadata.read))}}r.matches=s}return r.matches}function m_(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let a=uD(t,n,r,e);for(let s=0;s<o.length;s+=2){let c=o[s];if(c>0)i.push(a[s/2]);else{let l=o[s+1],d=n[-c];for(let f=ft;f<d.length;f++){let p=d[f];p[oo]===p[Pt]&&m_(p[oe],p,l,i)}if(d[ra]!==null){let f=d[ra];for(let p=0;p<f.length;p++){let _=f[p];m_(_[oe],_,l,i)}}}}}return i}function X_(t,n){return t[Yi].queries[n].queryList}function fD(t,n,e){let i=new Si((e&4)===4);return xC(t,n,i,i.destroy),(n[Yi]??=new u_).queries.push(new d_(i))-1}function hD(t,n,e){let i=tt();return i.firstCreatePass&&(pD(i,new Pf(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),fD(i,le(),n)}function mD(t,n,e,i){let r=tt();if(r.firstCreatePass){let o=xt();pD(r,new Pf(n,e,i),o.index),o1(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return fD(r,le(),e)}function r1(t){return t.split(",").map(n=>n.trim())}function pD(t,n,e){t.queries===null&&(t.queries=new f_),t.queries.track(new h_(n,e))}function o1(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function J_(t,n){return t.queries.getByIndex(n)}function gD(t,n){let e=t[oe],i=J_(e,n);return i.crossesNgTemplate?m_(e,t,n,[]):uD(e,t,i,n)}function _D(t,n,e){let i,r=Ac(()=>{i._dirtyCounter();let o=a1(i,t);if(n&&o===void 0)throw new k(-951,!1);return o});return i=r[_t],i._dirtyCounter=S(0),i._flatValue=void 0,r}function ev(t){return _D(!0,!1,t)}function tv(t){return _D(!0,!0,t)}function vD(t,n){let e=t[_t];e._lView=le(),e._queryIndex=n,e._queryList=X_(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function a1(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[fe]&4)return n?void 0:Qt;let r=X_(e,i),o=gD(e,i);return r.reset(o,Kw),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function Mr(t){return!!t&&typeof t.then=="function"}function nv(t){return!!t&&typeof t.subscribe=="function"}var er=class{},Qf=class{};var Ff=class extends er{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=rC(n);this._bootstrapComponents=YA(o.bootstrap),this._r3Injector=Mg(n,e,[{provide:er,useValue:this},...i],Qc(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Lf=class extends Qf{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Ff(this.moduleType,n,[])}};var yl=class extends er{injector;instance=null;constructor(n){super();let e=new Qo([...n.providers,{provide:er,useValue:this}],n.parent||cs(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Ml(t,n,e=null){return new yl({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var s1=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=tg(!1,e.type),r=i.length>0?Ml([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=me({token:t,providedIn:"environment",factory:()=>new t(J(qe))})}return t})();function F(t){return Cl(()=>{let n=yD(t),e=re(C({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==x_.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(s1).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Di.Emulated,styles:t.styles||Qt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&xr("NgStandalone"),bD(e);let i=t.dependencies;return e.directiveDefs=Sw(i,c1),e.pipeDefs=Sw(i,oC),e.id=u1(e),e})}function c1(t){return no(t)||Zp(t)}function ae(t){return Cl(()=>({type:t.type,bootstrap:t.bootstrap||Qt,declarations:t.declarations||Qt,imports:t.imports||Qt,exports:t.exports||Qt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function l1(t,n){if(t==null)return io;let e={};for(let i in t)if(Object.hasOwn(t,i)){let r=t[i],o,a,s,c;Array.isArray(r)?(s=r[0],o=r[1],a=r[2]??o,c=r[3]||null):(o=r,a=r,s=Yf.None,c=null),e[o]=[i,s,c],n[o]=a}return e}function d1(t){if(t==null)return io;let n={};for(let e in t)Object.hasOwn(t,e)&&(n[t[e]]=e);return n}function P(t){return Cl(()=>{let n=yD(t);return bD(n),n})}function iv(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function yD(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||io,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||Qt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:l1(t.inputs,n),outputs:d1(t.outputs),debugInfo:null}}function bD(t){t.features?.forEach(n=>n(t))}function Sw(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function u1(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var CD=new w("");var rv=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=u(CD,{optional:!0})??[];injector=u(te);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Ft(this.injector,r);if(Mr(o))e.push(o);else if(nv(o)){let a=new Promise((s,c)=>{o.subscribe({complete:s,error:c})});e.push(a)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function ov(t){return n=>{n.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:t}}}function f1(t){return Object.getPrototypeOf(t.prototype).constructor}function Be(t){let n=f1(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,Jc)?n[Jc]:void 0,a=Object.hasOwn(n,el)?n[el]:void 0;if(Qi(t))r=o??a;else{if(o)throw new k(903,!1);r=a}if(r){if(e){i.push(r);let c=t;c.inputs=Lg(t.inputs),c.declaredInputs=Lg(t.declaredInputs),c.outputs=Lg(t.outputs);let l=r.hostBindings;l&&_1(t,l);let d=r.viewQuery,f=r.contentQueries;if(d&&p1(t,d),f&&g1(t,f),h1(t,r),iC(t.outputs,r.outputs),Qi(r)&&r.data.animation){let p=t.data;p.animation=(p.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let c=0;c<s.length;c++){let l=s[c];l&&l.ngInherit&&l(t),l===Be&&(e=!1)}}n=Object.getPrototypeOf(n)}m1(i)}function h1(t,n){for(let e in n.inputs){if(!Object.hasOwn(n.inputs,e)||Object.hasOwn(t.inputs,e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function m1(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=gs(r.hostAttrs,e=gs(e,r.hostAttrs))}}function Lg(t){return t===io?{}:t===Qt?[]:t}function p1(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function g1(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function _1(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function wD(t,n,e,i,r,o,a,s){if(e.firstCreatePass){t.mergedAttrs=gs(t.mergedAttrs,t.attrs);let d=t.tView=z_(2,t,r,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),fs(t,!1);let c=y1(e,n,t,i);df()&&U_(e,n,c,t),_s(c,n);let l=YS(c,n,c,t);n[i+ut]=l,G_(n,l),XO(l,t,n)}function v1(t,n,e,i,r,o,a,s,c,l,d){let f=e+ut,p;return n.firstCreatePass?(p=Ss(n,f,4,a||null,s||null),gg()&&rD(n,t,p,Fn(n.consts,l),FS),Lw(n,p)):p=n.data[f],wD(p,t,n,e,i,r,o,c),ol(p)&&W_(n,t,p),l!=null&&Kf(t,p,d),p}function bs(t,n,e,i,r,o,a,s,c,l,d){let f=e+ut,p;if(n.firstCreatePass){if(p=Ss(n,f,4,a||null,s||null),l!=null){let _=Fn(n.consts,l);p.localNames=[];for(let b=0;b<_.length;b+=2)p.localNames.push(_[b],-1)}}else p=n.data[f];return wD(p,t,n,e,i,r,o,c),l!=null&&Kf(t,p,d),p}function jn(t,n,e,i,r,o,a,s){let c=le(),l=tt(),d=Fn(l.consts,o);return v1(c,l,t,n,e,i,r,d,void 0,a,s),jn}function Il(t,n,e,i,r,o,a,s){let c=le(),l=tt(),d=Fn(l.consts,o);return bs(c,l,t,n,e,i,r,d,void 0,a,s),Il}var y1=b1;function b1(t,n,e,i){return uf(!0),n[Xe].createComment("")}var Xf=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var av=new w("");var Nl=new w("");function SD(){mp(()=>{let t="";throw new k(600,t)})}var C1=10;var Cn=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(Ln);afterRenderManager=u($f);zonelessEnabled=u(ll);rootEffectScheduler=u(mf);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new N;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(Cr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ge(e=>!e))}constructor(){u(nr,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=u(qe);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=te.NULL){return this._injector.get(H).run(()=>{if(ze(Ae.BootstrapComponentStart),!this._injector.get(rv).done){let E="";throw new k(405,E)}let s=no(e),c=this._injector.get(er),l=new ys(s,c);this.componentTypes.push(e);let{hostElement:d,directives:f,bindings:p}=w1(i),_=d||l.selector,b=l.create(r,[],_,c.injector,f,p),M=b.location.nativeElement,x=b.injector.get(av,null);return x?.registerApplication(M),b.onDestroy(()=>{this.detachView(b.hostView),ml(this.components,b),x?.unregisterApplication(M)}),this._loadComponent(b),ze(Ae.BootstrapComponentEnd,b),b})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ze(Ae.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(zf.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ze(Ae.ChangeDetectionEnd),new k(101,!1);let e=ce(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,ce(e),this.afterTick.next(),ze(Ae.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Mt,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<C1;){ze(Ae.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ze(Ae.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!al(r))continue;let o=i&&!this.zonelessEnabled?0:1;$S(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>al(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;ml(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Nl,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>ml(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new k(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function w1(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function ml(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function ee(t,n,e,i){let r=le(),o=ao();if(ni(r,o,n)){let a=tt(),s=sa();HR(s,r,t,n,e,i)}return ee}var p_=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let a=this.detach(i);this.attach(i,o),this.attach(r,a)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function Vg(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function S1(t,n,e,i){let r,o,a=0,s=t.length-1,c=void 0;if(Array.isArray(n)){ce(i);let l=n.length-1;for(ce(null);a<=s&&a<=l;){let d=t.at(a),f=n[a],p=Vg(a,d,a,f,e);if(p!==0){p<0&&t.updateValue(a,f),a++;continue}let _=t.at(s),b=n[l],M=Vg(s,_,l,b,e);if(M!==0){M<0&&t.updateValue(s,b),s--,l--;continue}let x=e(a,d),E=e(s,_),L=e(a,f);if(Object.is(L,E)){let W=e(l,b);Object.is(W,x)?(t.swap(a,s),t.updateValue(s,b),l--,s--):t.move(s,a),t.updateValue(a,f),a++;continue}if(r??=new Vf,o??=Ew(t,a,s,e),g_(t,r,a,L))t.updateValue(a,f),a++,s++;else if(o.has(L))r.set(x,t.detach(a)),s--;else{let W=t.create(a,n[a]);t.attach(a,W),a++,s++}}for(;a<=l;)Dw(t,r,e,a,n[a]),a++}else if(n!=null){ce(i);let l=n[Symbol.iterator]();ce(null);let d=l.next();for(;!d.done&&a<=s;){let f=t.at(a),p=d.value,_=Vg(a,f,a,p,e);if(_!==0)_<0&&t.updateValue(a,p),a++,d=l.next();else{r??=new Vf,o??=Ew(t,a,s,e);let b=e(a,p);if(g_(t,r,a,b))t.updateValue(a,p),a++,s++,d=l.next();else if(!o.has(b))t.attach(a,t.create(a,p)),a++,s++,d=l.next();else{let M=e(a,f);r.set(M,t.detach(a)),s--}}}for(;!d.done;)Dw(t,r,e,t.length,d.value),d=l.next()}for(;a<=s;)t.destroy(t.detach(s--));r?.forEach(l=>{t.destroy(l)})}function g_(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function Dw(t,n,e,i,r){if(g_(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function Ew(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var Vf=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function R(t,n,e,i,r,o,a,s){xr("NgControlFlow");let c=le(),l=tt(),d=Fn(l.consts,o);return bs(c,l,t,n,e,i,r,d,256,a,s),sv}function sv(t,n,e,i,r,o,a,s){xr("NgControlFlow");let c=le(),l=tt(),d=Fn(l.consts,o);return bs(c,l,t,n,e,i,r,d,512,a,s),sv}function O(t,n){xr("NgControlFlow");let e=le(),i=ao(),r=e[i]!==fn?e[i]:-1,o=r!==-1?jf(e,ut+r):void 0,a=0;if(ni(e,i,t)){let s=ce(null);try{if(o!==void 0&&ZS(o,a),t!==-1){let c=ut+t,l=jf(e,c),d=b_(e[oe],c),f=XS(l,d,e),p=El(e,d,n,{dehydratedView:f});xl(l,p,a,vs(d,f))}}finally{ce(s)}}else if(o!==void 0){let s=KS(o,a);s!==void 0&&(s[yt]=n)}}var __=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-ft}};function _a(t,n){return n}var v_=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function at(t,n,e,i,r,o,a,s,c,l,d,f,p){xr("NgControlFlow");let _=le(),b=tt(),M=c!==void 0,x=le(),E=s?a.bind(x[dn][yt]):a,L=new v_(M,E);x[ut+t]=L,bs(_,b,t+1,n,e,i,r,Fn(b.consts,o),256),M&&bs(_,b,t+2,c,l,d,f,Fn(b.consts,p),512)}var y_=class extends p_{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-ft}at(n){return this.getLView(n)[yt].$implicit}attach(n,e){let i=e[ea];this.needsIndexUpdate||=n!==this.length,xl(this.lContainer,e,n,vs(this.templateTNode,i)),D1(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,E1(this.lContainer,n),x1(this.lContainer,n)}create(n,e){let i=Nf(this.lContainer,this.templateTNode.tView.ssrId);return El(this.hostLView,this.templateTNode,new __(this.lContainer,e,n),{dehydratedView:i})}destroy(n){Wf(n[oe],n)}updateValue(n,e){this.getLView(n)[yt].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[yt].$index=n}getLView(n){return M1(this.lContainer,n)}};function st(t){let n=ce(null),e=bi();try{let i=le(),r=i[oe],o=i[e],a=e+1,s=jf(i,a);if(o.liveCollection===void 0){let l=b_(r,a);o.liveCollection=new y_(s,i,l)}else o.liveCollection.reset();let c=o.liveCollection;if(S1(c,t,o.trackByFn,n),c.updateIndexes(),o.hasEmptyBlock){let l=ao(),d=c.length===0;if(ni(i,l,d)){let f=e+2,p=jf(i,f);if(d){let _=b_(r,f),b=XS(p,_,i),M=El(i,_,void 0,{dehydratedView:b});xl(p,M,0,vs(_,b))}else r.firstUpdatePass&&hO(p),ZS(p,0)}}}finally{ce(n)}}function jf(t,n){return t[n]}function D1(t,n){if(t.length<=ft)return;let e=ft+n,i=t[e],r=i?i[Zi]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[qi];hR(o,r),uo.delete(i[Ki]),r.detachedLeaveAnimationFns=void 0}}function E1(t,n){if(t.length<=ft)return;let e=ft+n,i=t[e],r=i?i[Zi]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function x1(t,n){return vl(t,n)}function M1(t,n){return KS(t,n)}function b_(t,n){return Ju(t,n)}function T(t,n,e){let i=le(),r=ao();if(ni(i,r,n)){let o=tt(),a=sa();OS(a,i,t,n,i[Xe],e)}return T}function C_(t,n,e,i,r){q_(n,t,e,r?"class":"style",i)}function m(t,n,e,i){let r=le(),o=r[oe],a=t+ut,s=o.firstCreatePass?aD(a,r,2,n,FS,gg(),e,i):o.data[a];if(br(s)){let c=r[yi].tracingService;if(c&&c.componentCreate){let l=o.data[s.directiveStart+s.componentOffset];return c.componentCreate(tD(l),()=>(xw(t,n,r,s,i),m))}}return xw(t,n,r,s,i),m}function xw(t,n,e,i,r){if(LS(i,e,t,n,DD),ol(i)){let o=e[oe];W_(o,e,i),cS(o,i,e)}r!=null&&Kf(e,i)}function h(){let t=tt(),n=xt(),e=VS(n);return t.firstCreatePass&&sD(t,e),vg(e)&&yg(),pg(),e.classesWithoutHost!=null&&Jk(e)&&C_(t,e,le(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&eA(e)&&C_(t,e,le(),e.stylesWithoutHost,!1),h}function de(t,n,e,i){return m(t,n,e,i),h(),de}function Ve(t,n,e,i){let r=le(),o=r[oe],a=t+ut,s=o.firstCreatePass?FO(a,o,2,n,e,i):o.data[a];return LS(s,r,t,n,DD),i!=null&&Kf(r,s),Ve}function $e(){let t=xt(),n=VS(t);return vg(n)&&yg(),pg(),$e}function wn(t,n,e,i){return Ve(t,n,e,i),$e(),wn}var DD=(t,n,e,i,r)=>(uf(!0),pS(n[Xe],i,xg()));function Ie(){return le()}function pt(t,n,e){let i=le(),r=ao();if(ni(i,r,n)){let o=tt(),a=sa();PS(a,i,t,n,i[Xe],e)}return pt}var ul=void 0;function I1(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var N1=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],ul,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],ul,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",ul,ul,ul],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",I1],jg=Object.create(null);function Bn(t){let n=T1(t),e=Mw(n);if(e)return e;let i=n.split("-")[0];if(e=Mw(i),e)return e;if(i==="en")return N1;throw new k(701,!1)}function Mw(t){if(!(t in jg)){let n=Qn.ng&&Qn.ng.common&&Qn.ng.common.locales&&Qn.ng.common.locales[t];return n!==void 0&&(jg[t]=n),n}return jg[t]}var It={LocaleId:0,DayPeriodsFormat:1,DayPeriodsStandalone:2,DaysFormat:3,DaysStandalone:4,MonthsFormat:5,MonthsStandalone:6,Eras:7,FirstDayOfWeek:8,WeekendRange:9,DateFormat:10,TimeFormat:11,DateTimeFormat:12,NumberSymbols:13,NumberFormats:14,CurrencyCode:15,CurrencySymbol:16,CurrencyName:17,Currencies:18,Directionality:19,PluralCase:20,ExtraData:21};function T1(t){return t.toLowerCase().replace(/_/g,"-")}var Tl="en-US";var k1=Tl;function ED(t){typeof t=="string"&&(k1=t.toLowerCase().replace(/_/g,"-"))}function I(t,n,e){let i=le(),r=tt(),o=xt();return xD(r,i,i[Xe],o,t,n,e),I}function ho(t,n,e){let i=le(),r=tt(),o=xt();return(o.type&3||e)&&Q_(o,r,i,e,i[Xe],t,n,ua(o,i,n)),ho}function xD(t,n,e,i,r,o,a){let s=!0,c=null;if((i.type&3||a)&&(c??=ua(i,n,o),Q_(i,t,n,a,e,r,o,c)&&(s=!1)),s){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let f=0;f<d.length;f+=2){let p=d[f],_=d[f+1];c??=ua(i,n,o),kf(i,n,p,_,r,c)}if(l&&l.length)for(let f of l)c??=ua(i,n,o),kf(i,n,f,r,r,c)}}function D(t=1){return BC(t)}function A1(t,n){let e=null,i=eR(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?wS(t,o,!0):iR(i,o))return r}return e}function ke(t){let n=le()[dn][Jt];if(!n.projection){let e=t?t.length:1,i=n.projection=uC(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?A1(o,t):0;a!==null&&(r[a]?r[a].projectionNext=o:i[a]=o,r[a]=o)}o=o.next}}}function se(t,n=0,e,i,r,o){let a=le(),s=tt(),c=i?t+1:null;c!==null&&bs(a,s,c,i,r,o,null,e);let l=Ss(s,ut+t,16,null,e||null);l.projection===null&&(l.projection=n),wg();let f=!a[ea]||_g();a[dn][Jt].projection[l.projection]===null&&c!==null?R1(a,s,c):f&&!Uf(l)&&IR(s,a,l)}function R1(t,n,e){let i=ut+e,r=n.data[i],o=t[i],a=Nf(o,r.tView.ssrId),s=El(t,r,void 0,{dehydratedView:a});xl(o,s,0,vs(r,a))}function Wt(t,n,e,i){return mD(t,n,e,i),Wt}function nt(t,n,e){return hD(t,n,e),nt}function K(t){let n=le(),e=tt(),i=sf();sl(i+1);let r=J_(e,i);if(t.dirty&&wC(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=gD(n,i);t.reset(o,Kw),t.notifyOnChanges()}return!0}return!1}function Z(){return X_(le(),sf())}function Jf(t,n,e,i,r){return vD(n,mD(t,e,i,r)),Jf}function eh(t,n,e,i){return vD(t,hD(n,e,i)),eh}function th(t=1){sl(sf()+t)}function ii(t){let n=TC();return lg(n,ut+t)}function yf(t,n){return t<<17|n<<2}function ma(t){return t>>17&32767}function O1(t){return(t&2)==2}function P1(t,n){return t&131071|n<<17}function w_(t){return t|2}function Cs(t){return(t&131068)>>2}function Bg(t,n){return t&-131069|n<<2}function F1(t){return(t&1)===1}function S_(t){return t|1}function L1(t,n,e,i,r,o){let a=o?n.classBindings:n.styleBindings,s=ma(a),c=Cs(a);t[i]=e;let l=!1,d;if(Array.isArray(e)){let f=e;d=f[1],(d===null||ss(f,d)>0)&&(l=!0)}else d=e;if(r)if(c!==0){let p=ma(t[s+1]);t[i+1]=yf(p,s),p!==0&&(t[p+1]=Bg(t[p+1],i)),t[s+1]=P1(t[s+1],i)}else t[i+1]=yf(s,0),s!==0&&(t[s+1]=Bg(t[s+1],i)),s=i;else t[i+1]=yf(c,0),s===0?s=i:t[c+1]=Bg(t[c+1],i),c=i;l&&(t[i+1]=w_(t[i+1])),Iw(t,d,i,!0),Iw(t,d,i,!1),V1(n,d,t,i,o),a=yf(s,c),o?n.classBindings=a:n.styleBindings=a}function V1(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&ss(o,n)>=0&&(e[i+1]=S_(e[i+1]))}function Iw(t,n,e,i){let r=t[e+1],o=n===null,a=i?ma(r):Cs(r),s=!1;for(;a!==0&&(s===!1||o);){let c=t[a],l=t[a+1];j1(c,n)&&(s=!0,t[a+1]=i?S_(l):w_(l)),a=i?ma(l):Cs(l)}s&&(t[e+1]=i?w_(r):S_(r))}function j1(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?ss(t,n)>=0:!1}var wi={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function B1(t){return t.substring(wi.key,wi.keyEnd)}function U1(t){return H1(t),MD(t,ID(t,0,wi.textEnd))}function MD(t,n){let e=wi.textEnd;return e===n?-1:(n=wi.keyEnd=z1(t,wi.key=n,e),ID(t,n,e))}function H1(t){wi.key=0,wi.keyEnd=0,wi.value=0,wi.valueEnd=0,wi.textEnd=t.length}function ID(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function z1(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function va(t,n,e){return ND(t,n,e,!1),va}function z(t,n){return ND(t,n,null,!0),z}function bt(t){G1(Q1,$1,t,!0)}function $1(t,n){for(let e=U1(n);e>=0;e=MD(n,e))Ku(t,B1(n),!0)}function ND(t,n,e,i){let r=le(),o=tt(),a=of(2);if(o.firstUpdatePass&&kD(o,t,a,i),n!==fn&&ni(r,a,n)){let s=o.data[bi()];AD(o,s,r,r[Xe],t,r[a+1]=J1(n,e),i,a)}}function G1(t,n,e,i){let r=tt(),o=of(2);r.firstUpdatePass&&kD(r,null,o,i);let a=le();if(e!==fn&&ni(a,o,e)){let s=r.data[bi()];if(RD(s,i)&&!TD(r,o)){let c=i?s.classesWithoutHost:s.stylesWithoutHost;c!==null&&(e=$u(c,e||"")),C_(r,s,a,e,i)}else X1(r,s,a,a[Xe],a[o+1],a[o+1]=Z1(t,n,e),i,o)}}function TD(t,n){return n>=t.expandoStartIndex}function kD(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[bi()],a=TD(t,e);RD(o,i)&&n===null&&!a&&(n=!1),n=W1(r,o,n,i),L1(r,o,n,e,a,i)}}function W1(t,n,e,i){let r=FC(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Ug(null,t,n,e,i),e=bl(e,n.attrs,i),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==r)if(e=Ug(r,t,n,e,i),o===null){let c=q1(t,n,i);c!==void 0&&Array.isArray(c)&&(c=Ug(null,t,n,c[1],i),c=bl(c,n.attrs,i),Y1(t,n,i,c))}else o=K1(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function q1(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Cs(i)!==0)return t[ma(i)]}function Y1(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[ma(r)]=i}function K1(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let a=t[o].hostAttrs;i=bl(i,a,e)}return bl(i,n.attrs,e)}function Ug(t,n,e,i,r){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],i=bl(i,o.hostAttrs,r),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function bl(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?r=a:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Ku(t,a,e?!0:n[++o]))}return t===void 0?null:t}function Z1(t,n,e){if(e==null||e==="")return Qt;let i=[],r=ti(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function Q1(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Ku(t,i,e)}function X1(t,n,e,i,r,o,a,s){r===fn&&(r=Qt);let c=0,l=0,d=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;d!==null||f!==null;){let p=c<r.length?r[c+1]:void 0,_=l<o.length?o[l+1]:void 0,b=null,M;d===f?(c+=2,l+=2,p!==_&&(b=f,M=_)):f===null||d!==null&&d<f?(c+=2,b=d):(l+=2,b=f,M=_),b!==null&&AD(t,n,e,i,b,M,a,s),d=c<r.length?r[c]:null,f=l<o.length?o[l]:null}}function AD(t,n,e,i,r,o,a,s){if(!(n.type&3))return;let c=t.data,l=c[s+1],d=F1(l)?Nw(c,n,e,r,Cs(l),a):void 0;if(!Bf(d)){Bf(o)||O1(l)&&(o=Nw(c,null,e,r,s,a));let f=cg(bi(),e);TR(i,a,f,r,o)}}function Nw(t,n,e,i,r,o){let a=n===null,s;for(;r>0;){let c=t[r],l=Array.isArray(c),d=l?c[1]:c,f=d===null,p=e[r+1];p===fn&&(p=f?Qt:void 0);let _=f?Zu(p,i):d===i?p:void 0;if(l&&!Bf(_)&&(_=Zu(c,i)),Bf(_)&&(s=_,a))return s;let b=t[r+1];r=a?ma(b):Cs(b)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(s=Zu(c,i))}return s}function Bf(t){return t!==void 0}function J1(t,n){return t==null||t===""||(typeof n=="string"?t=ti(t)+n:typeof t=="object"&&(t=Qc(ti(t)))),t}function RD(t,n){return(t.flags&(n?8:16))!==0}function v(t,n=""){let e=le(),i=tt(),r=t+ut,o=i.firstCreatePass?Ss(i,r,1,n,null):i.data[r],a=eP(i,e,o,n);e[r]=a,df()&&U_(i,e,a,o),fs(o,!1)}var eP=(t,n,e,i)=>(uf(!0),BA(n[Xe],i));function tP(t,n,e,i=""){return ni(t,ao(),e)?n+as(e)+i:fn}function nP(t,n,e,i,r,o=""){let a=kC(),s=nD(t,a,e,r);return of(2),s?n+as(e)+i+as(r)+o:fn}function G(t){return pe("",t),G}function pe(t,n,e){let i=le(),r=tP(i,t,n,e);return r!==fn&&OD(i,bi(),r),pe}function ya(t,n,e,i,r){let o=le(),a=nP(o,t,n,e,i,r);return a!==fn&&OD(o,bi(),a),ya}function OD(t,n,e){let i=cg(n,t);UA(t[Xe],i,e)}function Ds(t,n,e){pf(n)&&(n=n());let i=le(),r=ao();if(ni(i,r,n)){let o=tt(),a=sa();OS(a,i,t,n,i[Xe],e)}return Ds}function kl(t,n){let e=pf(t);return e&&t.set(n),e}function Es(t,n){let e=le(),i=tt(),r=xt();return xD(i,e,e[Xe],r,t,n),Es}function Tw(t,n,e){let i=tt();i.firstCreatePass&&PD(n,i.data,i.blueprint,Qi(t),e)}function PD(t,n,e,i,r){if(t=$t(t),Array.isArray(t))for(let o=0;o<t.length;o++)PD(t[o],n,e,i,r);else{let o=tt(),a=le(),s=xt(),c=Zo(t)?t:$t(t.provide),l=ig(t),d=s.providerIndexes&1048575,f=s.directiveStart,p=s.providerIndexes>>20;if(Zo(t)||!t.multi){let _=new fa(l,r,ie,null),b=zg(c,n,r?d:d+p,f);b===-1?(Wg(If(s,a),o,c),Hg(o,t,n.length),n.push(c),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(_),a.push(_)):(e[b]=_,a[b]=_)}else{let _=zg(c,n,d+p,f),b=zg(c,n,d,d+p),M=_>=0&&e[_],x=b>=0&&e[b];if(r&&!x||!r&&!M){Wg(If(s,a),o,c);let E=oP(r?rP:iP,e.length,r,i,l,t);!r&&x&&(e[b].providerFactory=E),Hg(o,t,n.length,0),n.push(c),s.directiveStart++,s.directiveEnd++,r&&(s.providerIndexes+=1048576),e.push(E),a.push(E)}else{let E=FD(e[r?b:_],l,!r&&i);Hg(o,t,_>-1?_:b,E)}!r&&i&&x&&e[b].componentProviders++}}}function Hg(t,n,e,i){let r=Zo(n),o=_C(n);if(r||o){let c=(o?$t(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(e);d===-1?l.push(e,[i,c]):l[d+1].push(i,c)}else l.push(e,c)}}}function FD(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function zg(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function iP(t,n,e,i,r){return D_(this.multi,[])}function rP(t,n,e,i,r){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,c=pl(i,i[oe],this.providerFactory.index,r);a=c.slice(0,s),D_(o,a);for(let l=s;l<c.length;l++)a.push(c[l])}else a=[],D_(o,a);return a}function D_(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function oP(t,n,e,i,r,o){let a=new fa(t,e,ie,null);return a.multi=[],a.index=n,a.componentProviders=0,FD(a,r,i&&!e),a}function Re(t,n){return e=>{e.providersResolver=(i,r)=>Tw(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>Tw(i,r?r(n):n,!0))}}function mo(t,n){let e=rf()+t,i=le();return i[e]===fn?Z_(i,e,n()):vO(i,e)}function Ir(t,n,e){return aP(le(),rf(),t,n,e)}function LD(t,n){let e=t[n];return e===fn?void 0:e}function aP(t,n,e,i,r,o){let a=n+e;return ni(t,a,r)?Z_(t,a+1,o?i.call(o,r):i(r)):LD(t,a+1)}function sP(t,n,e,i,r,o,a){let s=n+e;return nD(t,s,r,o)?Z_(t,s+2,a?i.call(a,r,o):i(r,o)):LD(t,s+2)}function ba(t,n){let e=tt(),i,r=t+ut;e.firstCreatePass?(i=cP(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=to(i.type,!0)),a,s=cn(ie);try{let c=Mf(!1),l=o();return Mf(c),dg(e,le(),r,l),l}finally{cn(s)}}function cP(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function Ca(t,n,e,i){let r=t+ut,o=le(),a=lg(o,r);return lP(o,r)?sP(o,rf(),n,a.transform,e,i,a):a.transform(e,i)}function lP(t,n){return t[oe].data[n].pure}function cv(t,n){return Zf(t,n)}var VD=(()=>{class t{applicationErrorHandler=u(Ln);appRef=u(Cn);taskService=u(Cr);ngZone=u(H);zonelessEnabled=u(ll);tracing=u(nr,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ue;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Kc):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(Ag,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?$C:Ig;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Kc+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function jD(){return[{provide:$i,useExisting:VD},{provide:H,useClass:Zc},{provide:ll,useValue:!0}]}var lv=(()=>{class t{compileModuleSync(e){return new Lf(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function dP(){return typeof $localize<"u"&&$localize.locale||Tl}var wa=new w("",{factory:()=>u(wa,{optional:!0,skipSelf:!0})||dP()});function ct(t,n){return Ac(t,n?.equal)}function Se(t){return S0(t)}var BD=class t extends Error{_brand;constructor(n){super(n)}static IDLE=new t("IDLE");static LOADING=new t("LOADING")},uP=t=>t;function nh(t,n){if(typeof t=="function"){let e=yp(t,uP,n?.equal);return UD(e,n?.debugName,n?.set)}else{let e=yp(t.source,t.computation,t.equal);return UD(e,t.debugName,t.set)}}function UD(t,n,e){let i=t[_t],r=t;if(e!==void 0){let o=a=>bp(i,a);r.set=a=>e(a,o),r.update=a=>e(a(Se(t)),o)}else r.set=o=>bp(i,o),r.update=o=>w0(i,o);return r.asReadonly=ff.bind(t),r}var WD=Symbol("InputSignalNode#UNSET"),EP=re(C({},Rc),{transformFn:void 0,applyValueToInputSignal(t,n){jo(t,n)}});function qD(t,n){let e=Object.create(EP);e.value=t,e.transformFn=n?.transform;function i(){if(Yr(e),e.value===WD){let r=null;throw new k(-950,r)}return e.value}return i[_t]=e,i}var ri=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>wl(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},YD=(()=>{let t=new w("");return t.__NG_ELEMENT_ID__=n=>{let e=xt();if(e===null)throw new k(-204,!1);if(e.type&2)return e.value;if(n&8)return null;throw new k(-204,!1)},t})();function mv(t){return xP(t)?t.default:t}function xP(t){return t&&typeof t=="object"&&"default"in t}function HD(t,n){return qD(t,n)}function MP(t){return qD(WD,t)}var Sa=(HD.required=MP,HD);function zD(t,n){return ev(n)}function IP(t,n){return tv(n)}var Rl=(zD.required=IP,zD);function $D(t,n){return ev(n)}function NP(t,n){return tv(n)}var KD=($D.required=NP,$D);var TP=1e4;var AX=TP-1e3;var De=(()=>{class t{static __NG_ELEMENT_ID__=kP}return t})();function kP(t){return AP(xt(),le(),(t&16)===16)}function AP(t,n,e){if(br(t)&&!e){let i=ei(t.index,n);return new fo(i,i)}else if(t.type&175){let i=n[dn];return new fo(i,n)}return null}var uv=new w(""),RP=new w("");function Al(t){return!t.moduleRef}function OP(t){let n=Al(t)?t.r3Injector:t.moduleRef.injector,e=n.get(H);return e.run(()=>{Al(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Ln),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Al(t)){let o=()=>n.destroy(),a=t.platformInjector.get(uv);a.add(o),n.onDestroy(()=>{r.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(uv);a.add(o),t.moduleRef.onDestroy(()=>{ml(t.allPlatformModules,t.moduleRef),r.unsubscribe(),a.delete(o)})}return FP(i,e,()=>{let o=n.get(Cr),a=o.add(),s=n.get(rv);return s.runInitializers(),s.donePromise.then(()=>{let c=n.get(wa,Tl);if(ED(c||Tl),!n.get(RP,!0))return Al(t)?n.get(Cn):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Al(t)){let d=n.get(Cn);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return PP?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var PP;function FP(t,n,e){try{let i=e();return Mr(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var ih=null;function LP(t=[],n){return te.create({name:n,providers:[{provide:il,useValue:"platform"},{provide:uv,useValue:new Set([()=>ih=null])},...t]})}function VP(t=[]){if(ih)return ih;let n=LP(t);return ih=n,SD(),jP(n),n}function jP(t){let n=t.get(hf,null);Ft(t,()=>{n?.forEach(e=>e())})}function ZD(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;ze(Ae.BootstrapApplicationStart);try{let o=r?.injector??VP(i),a=[jD(),WC,...e||[]],s=new yl({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return OP({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{ze(Ae.BootstrapApplicationEnd)}}function j(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function ir(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var dv=Symbol("NOT_SET"),QD=new Set,BP=re(C({},Rc),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:dv,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==dv&&!Za(this))return this.signal;try{for(let r of this.cleanup??QD)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=hr(this),i;try{i=this.userFn.apply(null,n)}finally{Kr(this,e)}return(this.value===dv||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),fv=class extends gl{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,a=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(Et),a),this.scheduler=r;for(let s of L_){let c=e[s];if(c===void 0)continue;let l=Object.create(BP);l.sequence=this,l.phase=s,l.userFn=c,l.dirty=!0,l.signal=()=>(Yr(l),l.value),l.signal[_t]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[s]=l,this.hooks[s]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??QD)e()}finally{Zr(n)}}};function pv(t,n){let e=n?.injector??u(te),i=e.get($i),r=e.get($f),o=e.get(nr,null,{optional:!0});r.impl??=e.get(V_);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(hs,null,{optional:!0}),c=new fv(r.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function rh(t,n){let e=no(t),i=n.elementInjector||cs();return new ys(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var XD=null;function oi(){return XD}function gv(t){XD??=t}var Ol=class{},xs=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=me({token:t,factory:()=>u(JD),providedIn:"platform"})}return t})();var JD=(()=>{class t extends xs{_location;_history;_doc=u($);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return oi().getBaseHref(this._doc)}onPopState(e){let i=oi().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=oi().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=me({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function nE(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function eE(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function go(t){return t&&t[0]!=="?"?`?${t}`:t}var Ms=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=me({token:t,factory:()=>u(HP),providedIn:"root"})}return t})(),UP=new w(""),HP=(()=>{class t extends Ms{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??u($).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return nE(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+go(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let a=this.prepareExternalUrl(r+go(o));this._platformLocation.pushState(e,i,a)}replaceState(e,i,r,o){let a=this.prepareExternalUrl(r+go(o));this._platformLocation.replaceState(e,i,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(J(xs),J(UP,8))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ai=(()=>{class t{_subject=new N;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=GP(eE(tE(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+go(i))}normalize(e){return t.stripTrailingSlash($P(this._basePath,tE(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+go(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+go(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=go;static joinWithSlash=nE;static stripTrailingSlash=eE;static \u0275fac=function(i){return new(i||t)(J(Ms))};static \u0275prov=me({token:t,factory:()=>zP(),providedIn:"root"})}return t})();function zP(){return new ai(J(Ms))}function $P(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function tE(t){return t.replace(/\/index\.html$/,"")}function GP(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var tn=(function(t){return t[t.Format=0]="Format",t[t.Standalone=1]="Standalone",t})(tn||{}),Je=(function(t){return t[t.Narrow=0]="Narrow",t[t.Abbreviated=1]="Abbreviated",t[t.Wide=2]="Wide",t[t.Short=3]="Short",t})(Je||{}),Sn=(function(t){return t[t.Short=0]="Short",t[t.Medium=1]="Medium",t[t.Long=2]="Long",t[t.Full=3]="Full",t})(Sn||{}),Tr={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function rE(t){return Bn(t)[It.LocaleId]}function oE(t,n,e){let i=Bn(t),r=[i[It.DayPeriodsFormat],i[It.DayPeriodsStandalone]],o=si(r,n);return si(o,e)}function aE(t,n,e){let i=Bn(t),r=[i[It.DaysFormat],i[It.DaysStandalone]],o=si(r,n);return si(o,e)}function sE(t,n,e){let i=Bn(t),r=[i[It.MonthsFormat],i[It.MonthsStandalone]],o=si(r,n);return si(o,e)}function cE(t,n){let i=Bn(t)[It.Eras];return si(i,n)}function Pl(t,n){let e=Bn(t);return si(e[It.DateFormat],n)}function Fl(t,n){let e=Bn(t);return si(e[It.TimeFormat],n)}function Ll(t,n){let i=Bn(t)[It.DateTimeFormat];return si(i,n)}function Vl(t,n){let e=Bn(t),i=e[It.NumberSymbols][n];if(typeof i>"u"){if(n===Tr.CurrencyDecimal)return e[It.NumberSymbols][Tr.Decimal];if(n===Tr.CurrencyGroup)return e[It.NumberSymbols][Tr.Group]}return i}function lE(t){if(!t[It.ExtraData])throw new k(2303,!1)}function dE(t){let n=Bn(t);return lE(n),(n[It.ExtraData][2]||[]).map(i=>typeof i=="string"?_v(i):[_v(i[0]),_v(i[1])])}function uE(t,n,e){let i=Bn(t);lE(i);let r=[i[It.ExtraData][0],i[It.ExtraData][1]],o=si(r,n)||[];return si(o,e)||[]}function si(t,n){for(let e=n;e>-1;e--)if(typeof t[e]<"u")return t[e];throw new k(2304,!1)}function _v(t){let[n,e]=t.split(":");return{hours:+n,minutes:+e}}var qP=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,oh=Object.create(null),YP=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/,KP=256;function fE(t,n,e,i){let r=oF(t);ZP(n),n=Nr(e,n)||n;let a=[],s;for(;n;)if(s=YP.exec(n),s){a=a.concat(s.slice(1));let d=a.pop();if(!d)break;n=d}else{a.push(n);break}let c=r.getTimezoneOffset();i&&(c=mE(i,c),r=rF(r,i));let l="";return a.forEach(d=>{let f=nF(d);l+=f?f(r,e,c):d==="''"?"'":d.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),l}function ZP(t){if(t.length>KP)throw new k(2300,!1)}function dh(t,n,e){let i=new Date(0);return i.setFullYear(t,n,e),i.setHours(0,0,0),i}function Nr(t,n){let e=rE(t);if(oh[e]??=Object.create(null),oh[e][n])return oh[e][n];let i="";switch(n){case"shortDate":i=Pl(t,Sn.Short);break;case"mediumDate":i=Pl(t,Sn.Medium);break;case"longDate":i=Pl(t,Sn.Long);break;case"fullDate":i=Pl(t,Sn.Full);break;case"shortTime":i=Fl(t,Sn.Short);break;case"mediumTime":i=Fl(t,Sn.Medium);break;case"longTime":i=Fl(t,Sn.Long);break;case"fullTime":i=Fl(t,Sn.Full);break;case"short":let r=Nr(t,"shortTime"),o=Nr(t,"shortDate");i=ah(Ll(t,Sn.Short),[r,o]);break;case"medium":let a=Nr(t,"mediumTime"),s=Nr(t,"mediumDate");i=ah(Ll(t,Sn.Medium),[a,s]);break;case"long":let c=Nr(t,"longTime"),l=Nr(t,"longDate");i=ah(Ll(t,Sn.Long),[c,l]);break;case"full":let d=Nr(t,"fullTime"),f=Nr(t,"fullDate");i=ah(Ll(t,Sn.Full),[d,f]);break}return i&&(oh[e][n]=i),i}function ah(t,n){return n&&(t=t.replace(/\{([^}]+)}/g,function(e,i){return Object.hasOwn(n,i)?n[i]:e})),t}function Ei(t,n,e="-",i,r){let o="";(t<0||r&&t<=0)&&(r?t=-t+1:(t=-t,o=e));let a=String(t);for(;a.length<n;)a="0"+a;return i&&(a=a.slice(a.length-n)),o+a}function QP(t,n){return Ei(t,3).substring(0,n)}function Nt(t,n,e=0,i=!1,r=!1){return function(o,a){let s=XP(t,o);if((e>0||s>-e)&&(s+=e),t===3)s===0&&e===-12&&(s=12);else if(t===6)return QP(s,n);let c=Vl(a,Tr.MinusSign);return Ei(s,n,c,i,r)}}function XP(t,n){switch(t){case 0:return n.getFullYear();case 1:return n.getMonth();case 2:return n.getDate();case 3:return n.getHours();case 4:return n.getMinutes();case 5:return n.getSeconds();case 6:return n.getMilliseconds();case 7:return n.getDay();default:throw new k(2301,!1)}}function it(t,n,e=tn.Format,i=!1){return function(r,o){return JP(r,o,t,n,e,i)}}function JP(t,n,e,i,r,o){switch(e){case 2:return sE(n,r,i)[t.getMonth()];case 1:return aE(n,r,i)[t.getDay()];case 0:let a=t.getHours(),s=t.getMinutes();if(o){let l=dE(n),d=uE(n,r,i),f=l.findIndex(p=>{if(Array.isArray(p)){let[_,b]=p,M=a>=_.hours&&s>=_.minutes,x=a<b.hours||a===b.hours&&s<b.minutes;if(_.hours<b.hours){if(M&&x)return!0}else if(M||x)return!0}else if(p.hours===a&&p.minutes===s)return!0;return!1});if(f!==-1)return d[f]}return oE(n,r,i)[a<12?0:1];case 3:return cE(n,i)[t.getFullYear()<=0?0:1];default:let c=e;throw new k(2302,!1)}}function sh(t){return function(n,e,i){let r=-1*i,o=Vl(e,Tr.MinusSign),a=r>0?Math.floor(r/60):Math.ceil(r/60);switch(t){case 0:return(r>=0?"+":"")+Ei(a,2,o)+Ei(Math.abs(r%60),2,o);case 1:return"GMT"+(r>=0?"+":"")+Ei(a,1,o);case 2:return"GMT"+(r>=0?"+":"")+Ei(a,2,o)+":"+Ei(Math.abs(r%60),2,o);case 3:return i===0?"Z":(r>=0?"+":"")+Ei(a,2,o)+":"+Ei(Math.abs(r%60),2,o);default:throw new k(2310,!1)}}}var eF=0,lh=4;function tF(t){let n=dh(t,eF,1).getDay();return dh(t,0,1+(n<=lh?lh:lh+7)-n)}function hE(t){let n=t.getDay(),e=n===0?-3:lh-n;return dh(t.getFullYear(),t.getMonth(),t.getDate()+e)}function vv(t,n=!1){return function(e,i){let r;if(n){let o=new Date(e.getFullYear(),e.getMonth(),1).getDay()-1,a=e.getDate();r=1+Math.floor((a+o)/7)}else{let o=hE(e),a=tF(o.getFullYear()),s=o.getTime()-a.getTime();r=1+Math.round(s/6048e5)}return Ei(r,t,Vl(i,Tr.MinusSign))}}function ch(t,n=!1){return function(e,i){let o=hE(e).getFullYear();return Ei(o,t,Vl(i,Tr.MinusSign),n)}}var yv=Object.create(null);function nF(t){if(yv[t])return yv[t];let n;switch(t){case"G":case"GG":case"GGG":n=it(3,Je.Abbreviated);break;case"GGGG":n=it(3,Je.Wide);break;case"GGGGG":n=it(3,Je.Narrow);break;case"y":n=Nt(0,1,0,!1,!0);break;case"yy":n=Nt(0,2,0,!0,!0);break;case"yyy":n=Nt(0,3,0,!1,!0);break;case"yyyy":n=Nt(0,4,0,!1,!0);break;case"Y":n=ch(1);break;case"YY":n=ch(2,!0);break;case"YYY":n=ch(3);break;case"YYYY":n=ch(4);break;case"M":case"L":n=Nt(1,1,1);break;case"MM":case"LL":n=Nt(1,2,1);break;case"MMM":n=it(2,Je.Abbreviated);break;case"MMMM":n=it(2,Je.Wide);break;case"MMMMM":n=it(2,Je.Narrow);break;case"LLL":n=it(2,Je.Abbreviated,tn.Standalone);break;case"LLLL":n=it(2,Je.Wide,tn.Standalone);break;case"LLLLL":n=it(2,Je.Narrow,tn.Standalone);break;case"w":n=vv(1);break;case"ww":n=vv(2);break;case"W":n=vv(1,!0);break;case"d":n=Nt(2,1);break;case"dd":n=Nt(2,2);break;case"c":case"cc":n=Nt(7,1);break;case"ccc":n=it(1,Je.Abbreviated,tn.Standalone);break;case"cccc":n=it(1,Je.Wide,tn.Standalone);break;case"ccccc":n=it(1,Je.Narrow,tn.Standalone);break;case"cccccc":n=it(1,Je.Short,tn.Standalone);break;case"E":case"EE":case"EEE":n=it(1,Je.Abbreviated);break;case"EEEE":n=it(1,Je.Wide);break;case"EEEEE":n=it(1,Je.Narrow);break;case"EEEEEE":n=it(1,Je.Short);break;case"a":case"aa":case"aaa":n=it(0,Je.Abbreviated);break;case"aaaa":n=it(0,Je.Wide);break;case"aaaaa":n=it(0,Je.Narrow);break;case"b":case"bb":case"bbb":n=it(0,Je.Abbreviated,tn.Standalone,!0);break;case"bbbb":n=it(0,Je.Wide,tn.Standalone,!0);break;case"bbbbb":n=it(0,Je.Narrow,tn.Standalone,!0);break;case"B":case"BB":case"BBB":n=it(0,Je.Abbreviated,tn.Format,!0);break;case"BBBB":n=it(0,Je.Wide,tn.Format,!0);break;case"BBBBB":n=it(0,Je.Narrow,tn.Format,!0);break;case"h":n=Nt(3,1,-12);break;case"hh":n=Nt(3,2,-12);break;case"H":n=Nt(3,1);break;case"HH":n=Nt(3,2);break;case"m":n=Nt(4,1);break;case"mm":n=Nt(4,2);break;case"s":n=Nt(5,1);break;case"ss":n=Nt(5,2);break;case"S":n=Nt(6,1);break;case"SS":n=Nt(6,2);break;case"SSS":n=Nt(6,3);break;case"Z":case"ZZ":case"ZZZ":n=sh(0);break;case"ZZZZZ":n=sh(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":n=sh(1);break;case"OOOO":case"ZZZZ":case"zzzz":n=sh(2);break;default:return null}return yv[t]=n,n}function mE(t,n){t=t.replace(/:/g,"");let e=Date.parse("Jan 01, 1970 00:00:00 "+t)/6e4;return isNaN(e)?n:e}function iF(t,n){return t=new Date(t.getTime()),t.setMinutes(t.getMinutes()+n),t}function rF(t,n,e){let r=t.getTimezoneOffset(),o=mE(n,r);return iF(t,-1*(o-r))}function oF(t){if(iE(t))return t;if(typeof t=="number"&&!isNaN(t))return new Date(t);if(typeof t=="string"){if(t=t.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(t)){let[r,o=1,a=1]=t.split("-").map(s=>+s);return dh(r,o-1,a)}let e=parseFloat(t);if(!isNaN(t-e))return new Date(e);let i;if(i=t.match(qP))return aF(i)}let n=new Date(t);if(!iE(n))throw new k(2311,!1);return n}function aF(t){let n=new Date(0),e=0,i=0,r=t[8]?n.setUTCFullYear:n.setFullYear,o=t[8]?n.setUTCHours:n.setHours;t[9]&&(e=Number(t[9]+t[10]),i=Number(t[9]+t[11])),r.call(n,Number(t[1]),Number(t[2])-1,Number(t[3]));let a=Number(t[4]||0)-e,s=Number(t[5]||0)-i,c=Number(t[6]||0),l=Math.floor(parseFloat("0."+(t[7]||0))*1e3);return o.call(n,a,s,c,l),n}function iE(t){return t instanceof Date&&!isNaN(t.valueOf())}var bv=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u(te);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ie(Lt))};static \u0275dir=P({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Pe]})}return t})();function sF(t,n){return new k(2100,!1)}var cF="mediumDate",pE=new w(""),gE=new w(""),jl=(()=>{class t{locale;defaultTimezone;defaultOptions;constructor(e,i,r){this.locale=e,this.defaultTimezone=i,this.defaultOptions=r}transform(e,i,r,o){if(e==null||e===""||e!==e)return null;try{let a=i??this.defaultOptions?.dateFormat??cF,s=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return fE(e,a,o||this.locale,s)}catch(a){throw sF(t,a.message)}}static \u0275fac=function(i){return new(i||t)(ie(wa,16),ie(pE,24),ie(gE,24))};static \u0275pipe=iv({name:"date",type:t,pure:!0})}return t})();function Bl(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()!==n)continue;let a=o;try{a=decodeURIComponent(o)}catch{}return a.length>1&&a[0]==='"'&&a[a.length-1]==='"'&&(a=a.slice(1,-1)),a}return null}var Cv="browser";function _E(t){return t===Cv}var Ul=class{_doc;constructor(n){this._doc=n}manager},uh=(()=>{class t extends Ul{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(J($))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),mh=new w(""),xv=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(a=>{a.manager=this});let r=e.filter(a=>!(a instanceof uh));this._plugins=r.slice().reverse();let o=e.find(a=>a instanceof uh);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new k(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(J(mh),J(H))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),Sv="ng-app-id";function vE(t){for(let n of t)n.remove()}function yE(t,n){let e=n.createElement("style");return e.textContent=t,e}function fF(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Sv}="${n}"],link[${Sv}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(Sv),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Ev(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Mv=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,fF(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,yE);i?.forEach(r=>this.addUsage(r,this.external,Ev))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(vE(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])vE(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,yE(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Ev(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(J($),J(wr),J(co,8),J(la))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),Dv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Iv=/%COMP%/g;var CE="%COMP%",hF=`_nghost-${CE}`,mF=`_ngcontent-${CE}`,pF=!0,gF=new w("",{factory:()=>pF}),_F=new w("");function vF(t){return mF.replace(Iv,t)}function yF(t){return hF.replace(Iv,t)}function wE(t,n){return n.map(e=>e.replace(Iv,t))}var Nv=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(e,i,r,o,a,s,c=null,l=null,d=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=c,this.tracingService=l,this.cssVarNamespace=d??"",this.defaultRenderer=new Hl(e,a,s,this.tracingService,this.cssVarNamespace)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof hh?r.applyToHost(e):r instanceof zl&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let a=this.doc,s=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case Di.Emulated:o=new hh(c,l,i,this.appId,d,a,s,f,this.cssVarNamespace);break;case Di.ShadowDom:return new fh(c,e,i,a,s,this.nonce,f,this.cssVarNamespace,l);case Di.ExperimentalIsolatedShadowDom:return new fh(c,e,i,a,s,this.nonce,f,this.cssVarNamespace);default:o=new zl(c,l,i,d,a,s,f,this.cssVarNamespace);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(J(xv),J(pa),J(wr),J(gF),J($),J(H),J(co),J(nr,8),J(_F,8))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),Hl=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r,o=""){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r,this.cssVarNamespace=o}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Dv[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(bE(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){if(n){let r=bE(n)?n.content:n;if(i!=null&&i.parentNode!==r)throw new k(-5106,bF(i));r.insertBefore(e,i)}}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new k(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Dv[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Dv[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){let o=e.startsWith("--");o&&(e=e.replace("%NS%",this.cssVarNamespace)),o||r&(Ji.DashCase|Ji.Important)?n.style.setProperty(e,i,r&Ji.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){let r=e.startsWith("--");r&&(e=e.replace("%NS%",this.cssVarNamespace)),r||i&Ji.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=oi().getGlobalEventTarget(this.doc,n),!n))throw new k(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function bE(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}function bF(t){let n=t.textContent?.slice(0,50);return n?`${t.nodeName} ("${n}")`:t.nodeName}var fh=class extends Hl{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,a,s,c,l){super(n,r,o,s,c),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let d=i.styles;d=wE(i.id,d).map(p=>p.replace(/%NS%/g,c));for(let p of d){let _=document.createElement("style");a&&_.setAttribute("nonce",a),_.textContent=p,this.shadowRoot.appendChild(_)}let f=i.getExternalStyles?.();if(f)for(let p of f){let _=Ev(p,r);a&&_.setAttribute("nonce",a),this.shadowRoot.appendChild(_)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},zl=class extends Hl{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,a,s,c,l){super(n,o,a,s,c),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let d=i.styles,f=l?wE(l,d):d;this.styles=f.map(p=>p.replace(/%NS%/g,c)),this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&uo.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},hh=class extends zl{contentAttr;hostAttr;constructor(n,e,i,r,o,a,s,c,l){let d=r+"-"+i.id;super(n,e,i,o,a,s,c,l,d),this.contentAttr=vF(d),this.hostAttr=yF(d)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var ph=class t extends Ol{supportsDOMEvents=!0;static makeCurrent(){gv(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=CF();return e==null?null:wF(e)}resetBaseElement(){$l=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Bl(document.cookie,n)}},$l=null;function CF(){return $l=$l||document.head.querySelector("base"),$l?$l.getAttribute("href"):null}function wF(t){return new URL(t,document.baseURI).pathname}var SE=["alt","control","meta","shift"],SF={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},DF={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},DE=(()=>{class t extends Ul{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let a=t.parseEventName(i),s=t.eventCallback(a.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>oi().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),a="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),a="code."),SE.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),a+=l+".")}),a+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=a,c}static matchEventFullKeyCode(e,i){let r=SF[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),SE.forEach(a=>{if(a!==r){let s=DF[a];s(e)&&(o+=a+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(J($))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})();async function Tv(t,n,e){let i=C({rootComponent:t},EF(n,e));return ZD(i)}function EF(t,n){return{platformRef:n?.platformRef,appProviders:[...TF,...t?.providers??[]],platformProviders:NF}}function xF(){ph.makeCurrent()}function MF(){return new ln}function IF(){return M_(document),document}var NF=[{provide:la,useValue:Cv},{provide:hf,useValue:xF,multi:!0},{provide:$,useFactory:IF}];var TF=[{provide:il,useValue:"root"},{provide:ln,useFactory:MF},{provide:mh,useClass:uh,multi:!0},{provide:mh,useClass:DE,multi:!0},Nv,{provide:pa,useClass:Mv},{provide:Mv,useExisting:pa},xv,{provide:Mt,useExisting:Nv},[]];var Ar=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init();for(let[e,i]of n.headers.entries())this.headers.set(e,i),this.normalizedNames.set(e,n.normalizedNames.get(e))}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=n.op==="a"?(this.headers.get(e)||[]).slice():[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(o===void 0)this.headers.delete(e),this.normalizedNames.delete(e);else{let a=Array.isArray(o)?o:[o],s=this.headers.get(e);if(!s)return;s=s.filter(c=>a.indexOf(c)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var Rv=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Ov=class{encodeKey(n){return EE(n)}encodeValue(n){return EE(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function kF(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[a,s]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=e.get(a)||[];c.push(s),e.set(a,c)}),e}var AF=/%(\d[a-f0-9])/gi,RF={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function EE(t){return encodeURIComponent(t).replace(AF,(n,e)=>RF[e]??n)}function gh(t){return`${t}`}var kr=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Ov,n.fromString){if(n.fromObject)throw new k(2805,!1);this.map=kF(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(gh):[gh(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){if(this.map===null&&(this.map=new Map),this.cloneFrom!==null){this.cloneFrom.init();for(let[n,e]of this.cloneFrom.map.entries())this.map.set(n,e);this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=n.op==="a"?(this.map.get(n.param)||[]).slice():[];e.push(gh(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=(this.map.get(n.param)||[]).slice(),r=i.indexOf(gh(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null}}};function OF(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function xE(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function ME(t){return typeof Blob<"u"&&t instanceof Blob}function IE(t){return typeof FormData<"u"&&t instanceof FormData}function PF(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var kv="Content-Type",NE="Accept",AE="text/plain",RE="application/json",FF=`${RE}, ${AE}, */*`,Is=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(OF(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new k(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Ar,this.context??=new Rv,!this.params)this.params=new kr,this.urlWithParams=e;else{let a=this.params.toString();if(a.length===0)this.urlWithParams=e;else{let s=e,c="",l=e.indexOf("#");l!==-1&&(c=e.substring(l),s=e.substring(0,l));let d=s.indexOf("?"),f=d===-1?"?":d<s.length-1?"&":"";this.urlWithParams=s+f+a+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||xE(this.body)||ME(this.body)||IE(this.body)||PF(this.body)?this.body:this.body instanceof kr?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||IE(this.body)?null:ME(this.body)?this.body.type||null:xE(this.body)?null:typeof this.body=="string"?AE:this.body instanceof kr?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?RE:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,a=n.priority||this.priority,s=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,f=n.referrer??this.referrer,p=n.integrity||this.integrity,_=n.referrerPolicy||this.referrerPolicy,b=n.transferCache??this.transferCache,M=n.timeout??this.timeout,x=n.body!==void 0?n.body:this.body,E=n.withCredentials??this.withCredentials,L=n.reportProgress??this.reportProgress,W=n.reportUploadProgress??this.reportUploadProgress,Me=n.reportDownloadProgress??this.reportDownloadProgress,rt=n.headers||this.headers,Ze=n.params||this.params,_n=n.context??this.context;return n.setHeaders!==void 0&&(rt=Object.keys(n.setHeaders).reduce((Vi,Yn)=>Vi.set(Yn,n.setHeaders[Yn]),rt)),n.setParams&&(Ze=Object.keys(n.setParams).reduce((Vi,Yn)=>Vi.set(Yn,n.setParams[Yn]),Ze)),new t(e,i,x,{params:Ze,headers:rt,context:_n,reportProgress:L,reportUploadProgress:W,reportDownloadProgress:Me,responseType:r,withCredentials:E,transferCache:b,keepalive:o,cache:s,priority:a,timeout:M,mode:c,redirect:l,credentials:d,referrer:f,integrity:p,referrerPolicy:_})}},Ea=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Ea||{}),Gl=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new Ar,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Pv=class t extends Gl{constructor(n={}){super(n)}type=Ea.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Wl=class t extends Gl{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Ea.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Da=class extends Gl{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},LF=200;var VF=/^\)\]\}',?\n/,Aee=1024*1024,jF=new w("",{factory:()=>null}),BF=(()=>{class t{fetchImpl=u(Fv,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=u(H);destroyRef=u(Et);maxResponseSize=u(jF);handle(e){return new he(i=>{let r=new AbortController,o=!1,a={next:c=>{c.type===Ea.Response&&(o=!0),i.next(c)},error:c=>{o=!0,i.error(c)},complete:()=>{o=!0,i.complete()}};this.doRequest(e,r.signal,a).then(Lv,c=>a.error(new Da({error:c})));let s;return e.timeout&&(s=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{s!==void 0&&clearTimeout(s),!o&&!r.signal.aborted&&r.abort()}})}async doRequest(e,i,r){let o=this.createRequestInit(e),a;try{let x=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,C({signal:i},o)));UF(x),r.next({type:Ea.Sent}),a=await x}catch(x){r.error(new Da({error:x,status:x.status??0,statusText:x.statusText,url:e.urlWithParams,headers:x.headers}));return}let s=new Ar(a.headers),c=a.statusText,l=a.url||e.urlWithParams,d=a.status,f=null,p=e.reportProgress||e.reportDownloadProgress;if(p&&r.next(new Pv({headers:s,status:d,statusText:c,url:l})),a.body){let x=a.headers.get(kv)??"",E=a.headers.get("content-length"),L=E!==null?Number(E):NaN;this.maxResponseSize!==null&&Number.isFinite(L)&&L>this.maxResponseSize&&(await a.body.cancel(),TE(this.maxResponseSize));let W=[],Me=a.body.getReader(),rt=0,Ze,_n,Vi=typeof Zone<"u"&&Zone.current,Yn=!1;if(await this.ngZone.runOutsideAngular(async()=>{for(;;){if(this.destroyRef.destroyed){await Me.cancel(),Yn=!0;break}let{done:Kn,value:$r}=await Me.read();if(Kn)break;if(W.push($r),rt+=$r.length,this.maxResponseSize!==null&&rt>this.maxResponseSize&&(await Me.cancel(),TE(this.maxResponseSize)),p){_n=e.responseType==="text"?(_n??"")+(Ze??=kE(x)).decode($r,{stream:!0}):void 0;let xc=()=>r.next({type:Ea.DownloadProgress,total:Number.isFinite(L)?L:void 0,loaded:rt,partialText:_n});Vi?Vi.run(xc):xc()}}}),Yn){r.complete();return}let ji=this.concatChunks(W,rt);try{f=this.parseBody(e,ji,x,d)}catch(Kn){r.error(new Da({error:Kn,headers:new Ar(a.headers),status:a.status,statusText:a.statusText,url:a.url||e.urlWithParams}));return}}d===0&&(d=f?LF:0);let _=d>=200&&d<300,b=a.redirected,M=a.type;_?(r.next(new Wl({body:f,headers:s,status:d,statusText:c,url:l,redirected:b,responseType:M})),r.complete()):r.error(new Da({error:f,headers:s,status:d,statusText:c,url:l,redirected:b,responseType:M}))}parseBody(e,i,r,o){switch(e.responseType){case"json":let a=new TextDecoder().decode(i).replace(VF,"");if(a==="")return null;try{return JSON.parse(a)}catch(s){if(o<200||o>=300)return a;throw s}case"text":return kE(r).decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new k(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,a)=>i[o]=a.join(",")),e.headers.has(NE)||(i[NE]=FF),!e.headers.has(kv)){let o=e.detectContentTypeHeader();o!==null&&(i[kv]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let a of e)r.set(a,o),o+=a.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),Fv=class{};function Lv(){}function UF(t){t.then(Lv,Lv)}function TE(t){throw new k(-2825,!1)}var HF=/charset=\s*["']?([^;"'\s]+)["']?/i;function kE(t){let n=t.match(HF);if(n!==null)try{return new TextDecoder(n[1])}catch{}return new TextDecoder}var zF=new w("",{factory:()=>!0}),$F="XSRF-TOKEN",GF=new w("",{factory:()=>$F}),WF="X-XSRF-TOKEN",qF=new w("",{factory:()=>WF}),YF=(()=>{class t{cookieName=u(GF);doc=u($);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=Bl(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),KF=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=me({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=J(YF),r},providedIn:"root"})}return t})();function ZF(t,n){if(!u(zF)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=u(xs).href,{origin:o}=new URL(r),{origin:a}=new URL(t.url,o);if(o!==a)return n(t)}catch{return n(t)}let e=u(KF).getToken(),i=u(qF);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}function QF(t,n){return n(t)}function XF(t,n,e){return(i,r)=>Ft(e,()=>n(i,o=>t(o,r)))}var JF=new w("",{factory:()=>[ZF]}),OE=new w(""),eL=new w("",{factory:()=>!0});var tL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=me({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=J(BF),r},providedIn:"root"})}return t})();var nL=(()=>{class t{backend;injector;chain=null;pendingTasks=u(dl);contributeToStability=u(eL);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let r=this.injector.get(PE,null,{skipSelf:!0}),o=r!==null&&this.backend===r,a=this.injector.get(OE,[],o?{self:!0}:void 0),s=Array.from(new Set([...this.injector.get(JF),...a]));this.chain=s.reduceRight((c,l)=>XF(c,l,this.injector),QF)}let i=this.chain;if(this.contributeToStability){let r=this.pendingTasks.add();return Se(()=>i(e,o=>this.backend.handle(o))).pipe(qo(r))}else return Se(()=>i(e,r=>this.backend.handle(r)))}static \u0275fac=function(i){return new(i||t)(J(tL),J(qe))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),PE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=me({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=J(nL),r},providedIn:"root"})}return t})();function Av(t,n){return C({body:n},t)}var Vv=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof Is)o=e;else{let c;r.headers instanceof Ar?c=r.headers:c=new Ar(r.headers);let l;r.params&&(r.params instanceof kr?l=r.params:l=new kr({fromObject:r.params})),o=new Is(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let a=X(o).pipe(Wo(c=>this.handler.handle(c)));if(e instanceof Is||r.observe==="events")return a;let s=a.pipe(je(c=>c instanceof Wl));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return s.pipe(ge(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new k(2806,!1);return c.body}));case"blob":return s.pipe(ge(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new k(2807,!1);return c.body}));case"text":return s.pipe(ge(c=>{if(c.body!==null&&typeof c.body!="string")throw new k(2808,!1);return c.body}));default:return s.pipe(ge(c=>c.body))}case"response":return s;default:throw new k(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new kr().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Av(r,i))}post(e,i,r={}){return this.request("POST",e,Av(r,i))}put(e,i,r={}){return this.request("PUT",e,Av(r,i))}static \u0275fac=function(i){return new(i||t)(J(PE))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var FE=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(J($))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ql=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=me({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=J(rL),r},providedIn:"root"})}return t})(),rL=(()=>{class t extends ql{_doc=u($);sanitize(e,i){if(i==null)return null;switch(e){case Oe.NONE:return i;case Oe.HTML:return Dr(i,"HTML")?ti(i):R_(this._doc,String(i)).toString();case Oe.STYLE:return Dr(i,"Style")?ti(i):i;case Oe.SCRIPT:if(Dr(i,"Script"))return ti(i);throw new k(5200,!1);case Oe.URL:return Dr(i,"URL")?ti(i):Sl(String(i));case Oe.RESOURCE_URL:if(Dr(i,"ResourceURL"))return ti(i);throw new k(-5201,!1);default:throw new k(5202,!1)}}bypassSecurityTrustHtml(e){return I_(e)}bypassSecurityTrustStyle(e){return N_(e)}bypassSecurityTrustScript(e){return T_(e)}bypassSecurityTrustUrl(e){return k_(e)}bypassSecurityTrustResourceUrl(e){return A_(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var _e="primary",sd=Symbol("RouteTitle"),zv=class{params;constructor(n){this.params=n||{}}has(n){return Object.hasOwn(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function Ma(t){return new zv(t)}function jv(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function $E(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let c={},l=t.slice(0,i.length);return jv(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),a=i.slice(r+1);if(o.length+a.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let s={};return!jv(o,t.slice(0,o.length),s)||!jv(a,t.slice(t.length-a.length),s)?null:{consumed:t,posParams:s}}function wh(t){return new Promise((n,e)=>{t.pipe(pr()).subscribe({next:i=>n(i),error:i=>e(i)})})}function oL(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!rr(t[e],n[e]))return!1;return!0}function rr(t,n){let e=t?$v(t):void 0,i=n?$v(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!GE(t[r],n[r]))return!1;return!0}function $v(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function GE(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function aL(t){return t.length>0?t[t.length-1]:null}function Ta(t){return jc(t)?t:Mr(t)?ot(Promise.resolve(t)):X(t)}function WE(t){return jc(t)?wh(t):Promise.resolve(t)}var sL={exact:YE,subset:KE},qE={exact:cL,subset:lL,ignored:()=>!0},oy={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},As={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function ay(t,n,e){let i=t instanceof hn?t:n.parseUrl(t);return ct(()=>Gv(n.lastSuccessfulNavigation()?.finalUrl??new hn,i,C(C({},As),e)))}function Gv(t,n,e){return sL[e.paths](t.root,n.root,e.matrixParams)&&qE[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function cL(t,n){return rr(t,n)}function YE(t,n,e){if(!xa(t.segments,n.segments)||!yh(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!YE(t.children[i],n.children[i],e))return!1;return!0}function lL(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>GE(t[e],n[e]))}function KE(t,n,e){return ZE(t,n,n.segments,e)}function ZE(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!xa(r,e)||n.hasChildren()||!yh(r,e,i))}else if(t.segments.length===e.length){if(!xa(t.segments,e)||!yh(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!KE(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!xa(t.segments,r)||!yh(t.segments,r,i)||!t.children[_e]?!1:ZE(t.children[_e],n,o,i)}}function yh(t,n,e){return n.every((i,r)=>qE[e](t[r].parameters,i.parameters))}var hn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Ue([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Ma(this.queryParams),this._queryParamMap}toString(){return fL.serialize(this)}},Ue=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return bh(this)}},_o=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=Ma(this.parameters),this._parameterMap}toString(){return XE(this)}};function dL(t,n){return xa(t,n)&&t.every((e,i)=>rr(e.parameters,n[i].parameters))}function xa(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function uL(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===_e&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==_e&&(e=e.concat(n(r,i)))}),e}var Vs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:()=>new vo})}return t})(),vo=class{parse(n){let e=new qv(n);return new hn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Yl(n.root,!0)}`,i=pL(n.queryParams),r=typeof n.fragment=="string"?`#${hL(n.fragment)}`:"";return`${e}${i}${r}`}},fL=new vo;function bh(t){return t.segments.map(n=>XE(n)).join("/")}function Yl(t,n){if(!t.hasChildren())return bh(t);if(n){let e=t.children[_e]?Yl(t.children[_e],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==_e&&i.push(`${r}:${Yl(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=uL(t,(i,r)=>r===_e?[Yl(t.children[_e],!1)]:[`${r}:${Yl(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[_e]!=null?`${bh(t)}/${e[0]}`:`${bh(t)}/(${e.join("//")})`}}function QE(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function _h(t){return QE(t).replace(/%3B/gi,";")}function hL(t){return encodeURI(t)}function Wv(t){return QE(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Ch(t){return decodeURIComponent(t)}function VE(t){return Ch(t.replace(/\+/g,"%20"))}function XE(t){return`${Wv(t.path)}${mL(t.parameters)}`}function mL(t){return Object.entries(t).map(([n,e])=>`;${Wv(n)}=${Wv(e)}`).join("")}function pL(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${_h(e)}=${_h(r)}`).join("&"):`${_h(e)}=${_h(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var gL=/^[^\/()?;#]+/;function Bv(t){let n=t.match(gL);return n?n[0]:""}var _L=/^[^\/()?;=#]+/;function vL(t){let n=t.match(_L);return n?n[0]:""}var yL=/^[^=?&#]+/;function bL(t){let n=t.match(yL);return n?n[0]:""}var CL=/^[^&#]+/;function wL(t){let n=t.match(CL);return n?n[0]:""}var qv=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Ue([],{}):new Ue([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new k(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[_e]=new Ue(e,i)),r}parseSegment(){let n=Bv(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new k(4009,!1);return this.capture(n),new _o(Ch(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=vL(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=Bv(this.remaining);r&&(i=r,this.capture(i))}n[Ch(e)]=Ch(i)}parseQueryParam(n){let e=bL(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let a=wL(this.remaining);a&&(i=a,this.capture(i))}let r=VE(e),o=VE(i);if(Object.hasOwn(n,r)){let a=n[r];Array.isArray(a)||(a=[a],n[r]=a),a.push(o)}else n[r]=o}parseParens(n,e){let i=Object.create(null);for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Bv(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new k(4010,!1);let a;r.indexOf(":")>-1?(a=r.slice(0,r.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=_e);let s=this.parseChildren(e+1);i[a??_e]=Object.keys(s).length===1&&s[_e]?s[_e]:new Ue([],s),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new k(4011,!1)}};function JE(t){return t.segments.length>0?new Ue([],{[_e]:t}):t}function ex(t){let n=Object.create(null);for(let[i,r]of Object.entries(t.children)){let o=ex(r);if(i===_e&&o.segments.length===0&&o.hasChildren())for(let[a,s]of Object.entries(o.children))n[a]=s;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Ue(t.segments,n);return SL(e)}function SL(t){if(t.numberOfChildren===1&&t.children[_e]){let n=t.children[_e];return new Ue(t.segments.concat(n.segments),n.children)}return t}function yo(t){return t instanceof hn}function tx(t,n,e=null,i=null,r=new vo){let o=nx(t);return ix(o,n,e,i,r)}function nx(t){let n;function e(o){let a={};for(let c of o.children){let l=e(c);a[c.outlet]=l}let s=new Ue(o.url,a);return o===t&&(n=s),s}let i=e(t.root),r=JE(i);return n??r}function ix(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return Uv(o,o,o,e,i,r);let a=DL(n);if(a.toRoot())return Uv(o,o,new Ue([],{}),e,i,r);let s=EL(a,o,t),c=s.processChildren?Zl(s.segmentGroup,s.index,a.commands):ox(s.segmentGroup,s.index,a.commands);return Uv(o,s.segmentGroup,c,e,i,r)}function Sh(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Jl(t){return typeof t=="object"&&t!=null&&t.outlets}function jE(t,n,e){t||="\u0275";let i=new hn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function Uv(t,n,e,i,r,o){let a={};for(let[l,d]of Object.entries(i??{}))a[l]=Array.isArray(d)?d.map(f=>jE(l,f,o)):jE(l,d,o);let s;t===n?s=e:s=rx(t,n,e);let c=JE(ex(s));return new hn(c,a,r)}function rx(t,n,e){let i=Object.create(null);return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=rx(o,n,e)}),new Ue(t.segments,i)}var Dh=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&Sh(i[0]))throw new k(4003,!1);let r=i.find(Jl);if(r&&r!==aL(i))throw new k(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function DL(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new Dh(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,a)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let s={};return Object.entries(o.outlets).forEach(([c,l])=>{s[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:s}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:a===0?(o.split("/").forEach((s,c)=>{c==0&&s==="."||(c==0&&s===""?e=!0:s===".."?n++:s!=""&&r.push(s))}),r):[...r,o]},[]);return new Dh(e,n,i)}var Ts=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function EL(t,n,e){if(t.isAbsolute)return new Ts(n,!0,0);if(!e)return new Ts(n,!1,NaN);if(e.parent===null)return new Ts(e,!0,0);let i=Sh(t.commands[0])?0:1,r=e.segments.length-1+i;return xL(e,r,t.numberOfDoubleDots)}function xL(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new k(4005,!1);r=i.segments.length}return new Ts(i,!1,r-o)}function ML(t){return Jl(t[0])?t[0].outlets:{[_e]:t}}function ox(t,n,e){if(t??=new Ue([],{}),t.segments.length===0&&t.hasChildren())return Zl(t,n,e);let i=IL(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Ue(t.segments.slice(0,i.pathIndex),{});return o.children[_e]=new Ue(t.segments.slice(i.pathIndex),t.children),Zl(o,0,r)}else return i.match&&r.length===0?new Ue(t.segments,{}):i.match&&!t.hasChildren()?Yv(t,n,e):i.match?Zl(t,0,r):Yv(t,n,e)}function Zl(t,n,e){if(e.length===0)return new Ue(t.segments,{});{let i=ML(e),r=Object.create(null);if(Object.keys(i).some(o=>o!==_e)&&t.children[_e]&&t.numberOfChildren===1&&t.children[_e].segments.length===0){let o=Zl(t.children[_e],n,e);return new Ue(t.segments,o.children)}return Object.entries(i).forEach(([o,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(r[o]=ox(t.children[o],n,a))}),Object.entries(t.children).forEach(([o,a])=>{i[o]===void 0&&(r[o]=a)}),new Ue(t.segments,r)}}function IL(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let a=t.segments[r],s=e[i];if(Jl(s))break;let c=`${s}`,l=i<e.length-1?e[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!UE(c,l,a))return o;i+=2}else{if(!UE(c,{},a))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Yv(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Jl(o)){let c=NL(o.outlets);return new Ue(i,c)}if(r===0&&Sh(e[0])){let c=t.segments[n];i.push(new _o(c.path,BE(e[0]))),r++;continue}let a=Jl(o)?o.outlets[_e]:`${o}`,s=r<e.length-1?e[r+1]:null;a&&s&&Sh(s)?(i.push(new _o(a,BE(s))),r+=2):(i.push(new _o(a,{})),r++)}return new Ue(i,{})}function NL(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=Yv(new Ue([],{}),0,i))}),n}function BE(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function UE(t,n,e){return t==e.path&&rr(n,e.parameters)}var Ql="imperative",Ut=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Ut||{}),Hn=class{id;url;constructor(n,e){this.id=n,this.url=e}},Ia=class extends Hn{type=Ut.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Mi=class extends Hn{urlAfterRedirects;type=Ut.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},nn=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(nn||{}),ed=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(ed||{}),ci=class extends Hn{reason;code;type=Ut.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function ax(t){return t instanceof ci&&(t.code===nn.Redirect||t.code===nn.SupersededByNewNavigation)}var Or=class extends Hn{reason;code;type=Ut.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},Na=class extends Hn{error;target;type=Ut.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},td=class extends Hn{urlAfterRedirects;state;type=Ut.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Eh=class extends Hn{urlAfterRedirects;state;type=Ut.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},xh=class extends Hn{urlAfterRedirects;state;shouldActivate;type=Ut.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Mh=class extends Hn{urlAfterRedirects;state;type=Ut.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Ih=class extends Hn{urlAfterRedirects;state;type=Ut.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Nh=class{route;type=Ut.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Th=class{route;type=Ut.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},kh=class{snapshot;type=Ut.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Ah=class{snapshot;type=Ut.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Rh=class{snapshot;type=Ut.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Oh=class{snapshot;type=Ut.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Rs=class{},nd=class{},Os=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function TL(t){return!(t instanceof Rs)&&!(t instanceof Os)&&!(t instanceof nd)}var Ph=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new js(this.rootInjector)}},js=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Ph(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(J(qe))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Fh=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=Kv(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=Kv(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=Zv(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Zv(n,this._root).map(e=>e.value)}};function Kv(t,n){if(t===n.value)return n;for(let e of n.children){let i=Kv(t,e);if(i)return i}return null}function Zv(t,n){if(t===n.value)return[n];for(let e of n.children){let i=Zv(t,e);if(i.length)return i.unshift(n),i}return[]}var Un=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function Ns(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var id=class extends Fh{snapshot;constructor(n,e){super(n),this.snapshot=e,cy(this,n)}toString(){return this.snapshot.toString()}};function sx(t,n){let e=kL(t,n),i=new At([new _o("",{})]),r=new At({}),o=new At({}),a=new At({}),s=new At(""),c=new mn(i,r,a,s,o,_e,t,e.root);return c.snapshot=e.root,new id(new Un(c,[]),e)}function kL(t,n){let e={},i={},r={},a=new Ps([],e,r,"",i,_e,t,null,{},n);return new rd("",new Un(a,[]))}var mn=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;resources;_localInjector;pending;paramsSignal;queryParamsSignal;paramMapSignal;queryParamMapSignal;fragmentSignal;dataSignal;constructor(n,e,i,r,o,a,s,c){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=a,this.component=s,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(ge(l=>l[sd]))??X(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(ge(n=>Ma(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(ge(n=>Ma(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}_setPending(n){this._futureSnapshot=n,this.pending?.set(!0)}},AL="always";function sy(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:C(C({},n.params),t.params),data:C(C({},n.data),t.data),resolve:C(C(C(C({},t.data),n.data),r?.data),t._resolvedData)}:i={params:C({},t.params),data:C({},t.data),resolve:C(C({},t.data),t._resolvedData??{})},r&&lx(r)&&(i.resolve[sd]=r.title),i}var Ps=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;resources;get title(){return this.data?.[sd]}constructor(n,e,i,r,o,a,s,c,l,d){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=a,this.component=s,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Ma(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Ma(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},rd=class extends Fh{url;constructor(n,e){super(e),this.url=n,cy(this,e)}toString(){return cx(this._root)}};function cy(t,n){n.value._routerState=t,n.children.forEach(e=>cy(t,e))}function cx(t){let n=t.children.length>0?` { ${t.children.map(cx).join(", ")} } `:"";return`${t.value}${n}`}function Hv(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,rr(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),rr(n.params,e.params)||t.paramsSubject.next(e.params),oL(n.url,e.url)||t.urlSubject.next(e.url),rr(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Qv(t,n){let e=rr(t.params,n.params)&&dL(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||Qv(t.parent,n.parent))}function lx(t){return typeof t.title=="string"||t.title===null}var dx=new w(""),cd=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=_e;activateEvents=new A;deactivateEvents=new A;attachEvents=new A;detachEvents=new A;routerOutletData=Sa();parentContexts=u(js);location=u(Lt);changeDetector=u(De);inputBinder=u(Bh,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new k(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new k(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new k(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new k(4013,!1);this._activatedRoute=e;let r=this.location,a=e.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,c=new Xv(e,s,r.injector,this.routerOutletData);this.activated=r.createComponent(a,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Pe]})}return t})(),Xv=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===mn?this.route:n===js?this.childContexts:n===dx?this.outletData:this.parent.get(n,e)}},Bh=new w("");var ly=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&de(0,"router-outlet")},dependencies:[cd],encapsulation:2,changeDetection:1})}return t})();function dy(t){let n=t.children&&t.children.map(dy),e=n?re(C({},t),{children:n}):C({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==_e&&(e.component=ly),e}function RL(t,n,e){let i=new Set,r=od(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new id(r,n)}}function od(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._setPending(n.value);let o=OL(t,n,e,i);return new Un(r,o)}else{if(t.shouldAttach(n.value)){let a=t.retrieve(n.value);if(a!==null){let s=a.route;return s.value._setPending(n.value),s.children=n.children.map(c=>od(t,c,void 0,i)),s}}let r=PL(n.value);r._setPending(n.value),i.add(r);let o=n.children.map(a=>od(t,a,void 0,i));return new Un(r,o)}}function OL(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return od(t,r,o,i);return od(t,r,void 0,i)})}function PL(t){return new mn(new At(t.url),new At(t.params),new At(t.queryParams),new At(t.fragment),new At(t.data),t.outlet,t.component,t)}var Fs=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},ux="ngNavigationCancelingError";function Lh(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=yo(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=fx(!1,nn.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function fx(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[ux]=!0,e.cancellationCode=n,e}function FL(t){return hx(t)&&yo(t.url)}function hx(t){return!!t&&t[ux]}var Jv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),Hv(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=Ns(e);n.children.forEach(o=>{let a=o.value.outlet;this.deactivateRoutes(o,r[a],i),delete r[a]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let a=i.getContext(r.outlet);a&&this.deactivateChildRoutes(n,e,a.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Ns(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);if(i&&i.outlet){let a=i.outlet.detach(),s=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Ns(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=Ns(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Oh(o.value.snapshot))}),n.children.length&&this.forwardEvent(new Ah(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(Hv(r),r===o)if(r.component){let a=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,a.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let a=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let s=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),Hv(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=r,a.outlet&&a.outlet.activateWith(r,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,i)}},Vh=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},ks=class{component;route;constructor(n,e){this.component=n,this.route=e}};function LL(t,n,e){let i=t._root,r=n?n._root:null;return Kl(i,r,e,[i.value])}function VL(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Bs(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Wp(t)?t:n.get(t):i}function Kl(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=Ns(n);return t.children.forEach(a=>{jL(a,o[a.value.outlet],e,i.concat([a.value]),r),delete o[a.value.outlet]}),Object.entries(o).forEach(([a,s])=>Xl(s,e.getContext(a),e,r)),r}function jL(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,a=n?n.value:null,s=e?e.getContext(t.value.outlet):null;if(a&&o.routeConfig===a.routeConfig){let c=BL(a,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new Vh(i)):(o.data=a.data,o._resolvedData=a._resolvedData),o.component?Kl(t,n,s?s.children:null,i,r):Kl(t,n,e,i,r),c&&s&&s.outlet&&s.outlet.isActivated&&r.canDeactivateChecks.push(new ks(s.outlet.component,a))}else a&&Xl(n,s,e,r),r.canActivateChecks.push(new Vh(i)),o.component?Kl(t,null,s?s.children:null,i,r):Kl(t,null,e,i,r);return r}function BL(t,n,e){if(typeof e=="function")return Ft(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!xa(t.url,n.url);case"pathParamsOrQueryParamsChange":return!xa(t.url,n.url)||!rr(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Qv(t,n)||!rr(t.queryParams,n.queryParams);default:return!Qv(t,n)}}function Xl(t,n,e,i){let r=Ns(t),o=t.value;Object.entries(r).forEach(([a,s])=>{o.component?n?Xl(s,n.children.getContext(a),n.children,i):Xl(s,null,null,i):Xl(s,e?e.getContext(a):null,e,i)}),o.component?n&&n.outlet&&n.outlet.isActivated?i.canDeactivateChecks.push(new ks(n.outlet.component,o)):i.canDeactivateChecks.push(new ks(null,o)):i.canDeactivateChecks.push(new ks(null,o))}function ld(t){return typeof t=="function"}function UL(t){return typeof t=="boolean"}function HL(t){return t&&ld(t.canLoad)}function zL(t){return t&&ld(t.canActivate)}function $L(t){return t&&ld(t.canActivateChild)}function GL(t){return t&&ld(t.canDeactivate)}function WL(t){return t&&ld(t.canMatch)}function mx(t){return t instanceof $o||t?.name==="EmptyError"}var vh=Symbol("INITIAL_VALUE");function Ls(){return ht(t=>Bc(t.map(n=>n.pipe(Ot(1),dt(vh)))).pipe(ge(n=>{for(let e of n)if(e!==!0){if(e===vh)return vh;if(e===!1||qL(e))return e}return!0}),je(n=>n!==vh),Ot(1)))}function qL(t){return yo(t)||t instanceof Fs}function px(t){return t.aborted?X(void 0).pipe(Ot(1)):new he(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function gx(t){return Ee(px(t))}function YL(t){return Zt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?X(re(C({},n),{guardsResult:!0})):KL(o,e,i).pipe(Zt(a=>a&&UL(a)?ZL(e,r,t):X(a)),ge(a=>re(C({},n),{guardsResult:a})))})}function KL(t,n,e){return ot(t).pipe(Zt(i=>t2(i.component,i.route,e,n)),pr(i=>i!==!0,!0))}function ZL(t,n,e){return ot(n).pipe(Wo(i=>Xr(XL(i.route.parent,e),QL(i.route,e),e2(t,i.path),JL(t,i.route))),pr(i=>i!==!0,!0))}function QL(t,n){return t!==null&&n&&n(new Rh(t)),X(!0)}function XL(t,n){return t!==null&&n&&n(new kh(t)),X(!0)}function JL(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return X(!0);let i=e.map(r=>Go(()=>{let o=n._environmentInjector,a=Bs(r,o),s=zL(a)?a.canActivate(n,t):Ft(o,()=>a(n,t));return Ta(s).pipe(pr())}));return X(i).pipe(Ls())}function e2(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>VL(o)).filter(o=>o!==null).map(o=>Go(()=>{let a=o.guards.map(s=>{let c=o.node._environmentInjector,l=Bs(s,c),d=$L(l)?l.canActivateChild(e,t):Ft(c,()=>l(e,t));return Ta(d).pipe(pr())});return X(a).pipe(Ls())}));return X(r).pipe(Ls())}function t2(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return X(!0);let o=r.map(a=>{let s=n._environmentInjector,c=Bs(a,s),l=GL(c)?c.canDeactivate(t,n,e,i):Ft(s,()=>c(t,n,e,i));return Ta(l).pipe(pr())});return X(o).pipe(Ls())}function n2(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return X(!0);let a=o.map(s=>{let c=Bs(s,t),l=HL(c)?c.canLoad(n,e):Ft(t,()=>c(n,e)),d=Ta(l);return r?d.pipe(gx(r)):d});return X(a).pipe(Ls(),_x(i))}function _x(t){return lu(Bt(n=>{if(typeof n!="boolean")throw Lh(t,n)}),ge(n=>n===!0))}function i2(t,n,e,i,r,o){let a=n.canMatch;if(!a||a.length===0)return X(!0);let s=a.map(c=>{let l=Bs(c,t),d=WL(l)?l.canMatch(n,e,r):Ft(t,()=>l(n,e,r));return Ta(d).pipe(gx(o))});return X(s).pipe(Ls(),_x(i))}var Rr=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},ad=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function r2(t){throw new k(4e3,!1)}function o2(t){throw fx(!1,nn.GuardRejected)}var ey=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[_e])throw r2(`${n.redirectTo}`);r=r.children[_e]}}async applyRedirectCommands(n,e,i,r,o){let a=await a2(e,r,o);if(a instanceof hn)throw new ad(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,i);if(a[0]==="/")throw new ad(s);return s}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new hn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let s=o.substring(1);i[r]=e[s]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),a=Object.create(null);return Object.entries(e.children).forEach(([s,c])=>{a[s]=this.createSegmentGroup(n,c,i,r)}),new Ue(o,a)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new k(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function a2(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return wh(Ta(Ft(e,()=>i(n))))}function s2(t,n){return t.providers&&!t._injector&&(t._injector=Ml(t.providers,n,`Route: ${t.path}`)),t._injector??n}function xi(t){return t.outlet||_e}function c2(t,n){let e=t.filter(i=>xi(i)===n);return e.push(...t.filter(i=>xi(i)!==n)),e}var ty={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function vx(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function l2(t,n,e,i,r,o,a){let s=yx(t,n,e);if(!s.matched)return X(s);let c=vx(o(s));return i=s2(n,i),i2(i,n,e,r,c,a).pipe(ge(l=>l===!0?s:C({},ty)))}function yx(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?C({},ty):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||$E)(e,t,n);if(!r)return C({},ty);let o={};Object.entries(r.posParams??{}).forEach(([s,c])=>{o[s]=c.path});let a=r.consumed.length>0?C(C({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:a,positionalParamSegments:r.posParams??{}}}function HE(t,n,e,i,r){return e.length>0&&f2(t,e,i,r)?{segmentGroup:new Ue(n,u2(i,new Ue(e,t.children))),slicedSegments:[]}:e.length===0&&h2(t,e,i)?{segmentGroup:new Ue(t.segments,d2(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Ue(t.segments,t.children),slicedSegments:e}}function d2(t,n,e,i){let r={};for(let o of e)if(Uh(t,n,o)&&!i[xi(o)]){let a=new Ue([],{});r[xi(o)]=a}return C(C({},i),r)}function u2(t,n){let e={};e[_e]=n;for(let i of t)if(i.path===""&&xi(i)!==_e){let r=new Ue([],{});e[xi(i)]=r}return e}function f2(t,n,e,i){return e.some(r=>!Uh(t,n,r)||!(xi(r)!==_e)?!1:!(i!==void 0&&xi(r)===i))}function h2(t,n,e){return e.some(i=>Uh(t,n,i))}function Uh(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function m2(t,n,e){return n.length===0&&!t.children[e]}var ny=class{};async function p2(t,n,e,i,r,o,a,s){return new iy(t,n,e,i,r,a,o,s).recognize()}var g2=31,iy=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,a,s,c){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=c,this.applyRedirects=new ey(this.urlSerializer,this.urlTree)}noMatchError(n){return new k(4002,`'${n.segmentGroup}'`)}async recognize(){let n=HE(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Un(i,e),o=new rd("",r),a=tx(i,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(a),{state:o,tree:a}}async match(n){let e=new Ps([],Object.freeze({}),Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),_e,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,_e,e),rootSnapshot:e}}catch(i){if(i instanceof ad)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Rr?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let a=await this.processSegment(n,e,i,i.segments,r,!0,o);return a instanceof Un?[a]:[]}async processChildren(n,e,i,r){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let a=[];for(let c of o){let l=i.children[c],d=c2(e,c),f=await this.processSegmentGroup(n,d,l,c,r);a.push(...f)}let s=bx(a);return _2(s),s}async processSegment(n,e,i,r,o,a,s){for(let c of e)try{return await this.processSegmentAgainstRoute(c._injector??n,e,c,i,r,o,a,s)}catch(l){if(l instanceof Rr||mx(l))continue;throw l}if(m2(i,r,o))return new ny;throw new Rr(i)}async processSegmentAgainstRoute(n,e,i,r,o,a,s,c){if(xi(i)!==a&&(a===_e||!Uh(r,o,i)))throw new Rr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,a,c);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,a,c);throw new Rr(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,a,s){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:f,remainingSegments:p}=yx(e,r,o);if(!c)throw new Rr(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>g2&&(this.allowRedirects=!1));let _=this.createSnapshot(n,r,o,l,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let b=await this.applyRedirects.applyRedirectCommands(d,r.redirectTo,f,vx(_),n),M=await this.applyRedirects.lineralizeSegments(r,b);return this.processSegment(n,i,e,M.concat(p),a,!1,s)}createSnapshot(n,e,i,r,o){let a=new Ps(i,r,Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,y2(e),xi(e),e.component??e._loadedComponent??null,e,b2(e),n),s=sy(a,o,this.paramsInheritanceStrategy);return a.params=Object.freeze(s.params),a.data=Object.freeze(s.data),a}async matchSegmentAgainstRoute(n,e,i,r,o,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=W=>this.createSnapshot(n,i,W.consumedSegments,W.parameters,a),c=await wh(l2(e,i,r,n,this.urlSerializer,s,this.abortSignal));if(i.path==="**"&&(e.children={}),!c?.matched)throw new Rr(e);n=i._injector??n;let{routes:l}=await this.getChildConfig(n,i,r),d=i._loadedInjector??n,{parameters:f,consumedSegments:p,remainingSegments:_}=c,b=this.createSnapshot(n,i,p,f,a),{segmentGroup:M,slicedSegments:x}=HE(e,p,_,l,o);if(x.length===0&&M.hasChildren()){let W=await this.processChildren(d,l,M,b);return new Un(b,W)}if(l.length===0&&x.length===0)return new Un(b,[]);let E=xi(i)===o,L=await this.processSegment(d,l,M,x,E?_e:o,!0,b);return new Un(b,L instanceof Un?[L]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await wh(n2(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw o2(e)}return{routes:[],injector:n}}};function _2(t){t.sort((n,e)=>n.value.outlet===_e?-1:e.value.outlet===_e?1:n.value.outlet.localeCompare(e.value.outlet))}function v2(t){let n=t.value.routeConfig;return n&&n.path===""}function bx(t){let n=[],e=new Set;for(let i of t){if(!v2(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=bx(i.children);n.push(new Un(i.value,r))}return n.filter(i=>!e.has(i))}function y2(t){return t.data||{}}function b2(t){return t.resolve||{}}function C2(t,n,e,i,r,o,a){return Zt(async s=>{let{state:c,tree:l}=await p2(t,n,e,i,s.extractedUrl,r,o,a);return re(C({},s),{targetSnapshot:c,urlAfterRedirects:l})})}function w2(t){return Zt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return X(n);let r=new Set(i.map(s=>s.route)),o=new Set;for(let s of r)if(!o.has(s))for(let c of Cx(s))o.add(c);let a=0;return ot(o).pipe(Wo(s=>r.has(s)?S2(s,e,t):(s.data=sy(s,s.parent,t).resolve,X(void 0))),Bt(()=>a++),Ou(1),Zt(s=>a===o.size?X(n):vt))})}function Cx(t){let n=t.children.map(e=>Cx(e)).flat();return[t,...n]}function S2(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!lx(i)&&(r[sd]=i.title),Go(()=>(t.data=sy(t,t.parent,e).resolve,D2(r,t,n).pipe(ge(o=>(t._resolvedData=o,t.data=C(C({},t.data),o),null)))))}function D2(t,n,e){let i=$v(t);if(i.length===0)return X({});let r={};return ot(i).pipe(Zt(o=>E2(t[o],n,e).pipe(pr(),Bt(a=>{if(a instanceof Fs)throw Lh(new vo,a);r[o]=a}))),Ou(1),ge(()=>r),Jr(o=>mx(o)?vt:Vc(o)))}function E2(t,n,e){let i=n._environmentInjector,r=Bs(t,i),o=r.resolve?r.resolve(n,e):Ft(i,()=>r(n,e));return Ta(o)}var wx=new w("");function ry(t){return ht(n=>{let e=t(n);return e?ot(e).pipe(ge(()=>n)):X(n)})}var uy=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===_e);return i}getResolvedTitleForRoute(e){return e.data[sd]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:()=>u(Sx)})}return t})(),Sx=(()=>{class t extends uy{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(J(FE))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Us=new w("",{factory:()=>({})}),dd=new w(""),Dx=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=u(lv);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await WE(Ft(e,()=>i.loadComponent())),a=await xx(mv(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=a,a}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await Ex(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();async function Ex(t,n,e,i){let r=await WE(Ft(e,()=>t.loadChildren())),o=await xx(mv(r)),a;o instanceof Qf||Array.isArray(o)?a=o:a=await n.compileModuleAsync(o),i&&i(t);let s,c,l=!1,d;return Array.isArray(a)?(c=a,l=!0):(s=a.create(e).injector,d=a,c=s.get(dd,[],{optional:!0,self:!0}).flat()),{routes:c.map(dy),injector:s,factory:d}}async function xx(t){return t}var Hh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:()=>u(x2)})}return t})(),x2=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),Mx=new w("");var M2=()=>{},Ix=new w(""),Nx=(()=>{class t{currentNavigation=S(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=S(null);events=new N;transitionAbortWithErrorSubject=new N;configLoader=u(Dx);environmentInjector=u(qe);destroyRef=u(Et);urlSerializer=u(Vs);rootContexts=u(js);location=u(ai);inputBindingEnabled=u(Bh,{optional:!0})!==null;titleStrategy=u(uy);options=u(Us,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||AL;urlHandlingStrategy=u(Hh);createViewTransition=u(Mx,{optional:!0});navigationErrorHandler=u(Ix,{optional:!0});routerResourcesFeature=u(wx,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>X(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new Nh(r)),i=r=>this.events.next(new Th(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Se(()=>{this.transitions?.next(re(C({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new At(null),this.transitions.pipe(je(i=>i!==null),ht(i=>{let r=!0,o=!1,a=new AbortController,s=()=>!o&&this.currentTransition?.id===i.id;return X(i).pipe(ht(c=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",nn.SupersededByNewNavigation),vt;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:l?re(C({},l),{previousNavigation:null}):null,abort:()=>a.abort(),routesRecognizeHandler:c.routesRecognizeHandler,beforeActivateHandler:c.beforeActivateHandler});let d=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=c.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!d&&f!=="reload")return this.events.next(new Or(c.id,this.urlSerializer.serialize(c.rawUrl),"",ed.IgnoredSameUrlNavigation)),c.resolve(!1),vt;if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return X(c).pipe(ht(p=>(this.events.next(new Ia(p.id,this.urlSerializer.serialize(p.extractedUrl),p.source,p.restoredState)),p.id!==this.navigationId?vt:Promise.resolve(p))),C2(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,a.signal),Bt(p=>{i.targetSnapshot=p.targetSnapshot,i.urlAfterRedirects=p.urlAfterRedirects,this.currentNavigation.update(_=>(_.finalUrl=p.urlAfterRedirects,_)),this.events.next(new nd)}),ht(p=>ot(i.routesRecognizeHandler.deferredHandle??X(void 0)).pipe(ge(()=>p))),Bt(()=>{let p=new td(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(p)}));if(d&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:p,extractedUrl:_,source:b,restoredState:M,extras:x}=c,E=new Ia(p,this.urlSerializer.serialize(_),b,M);this.events.next(E);let L=sx(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=re(C({},c),{targetSnapshot:L,urlAfterRedirects:_,extras:re(C({},x),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(W=>(W.finalUrl=_,W)),X(i)}else return this.events.next(new Or(c.id,this.urlSerializer.serialize(c.extractedUrl),"",ed.IgnoredByUrlHandlingStrategy)),c.resolve(!1),vt}),ge(c=>{let l=new Eh(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);return this.events.next(l),this.currentTransition=i=re(C({},c),{guards:LL(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),i}),YL(c=>this.events.next(c)),ht(c=>{if(i.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw Lh(this.urlSerializer,c.guardsResult);let l=new xh(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);if(this.events.next(l),!s())return vt;if(!c.guardsResult)return this.cancelNavigationTransition(c,"",nn.GuardRejected),vt;if(c.guards.canActivateChecks.length===0)return X(c);let d=new Mh(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);if(this.events.next(d),!s())return vt;let f=!1;return X(c).pipe(w2(this.paramsInheritanceStrategy),Bt({next:()=>{f=!0;let p=new Ih(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(p)},complete:()=>{f||this.cancelNavigationTransition(c,"",nn.NoDataFromResolver)}}))}),ry(c=>{let l=f=>{let p=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let _=f._environmentInjector;p.push(this.configLoader.loadComponent(_,f.routeConfig).then(b=>{f.component=b}))}for(let _ of f.children)p.push(...l(_));return p},d=l(c.targetSnapshot.root);return d.length===0?X(c):ot(Promise.all(d).then(()=>c))}),ht(c=>{let{newlyCreatedRoutes:l,state:d}=RL(e.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=i=c=re(C({},c),{targetRouterState:d,newlyCreatedRoutes:l}),this.currentNavigation.update(f=>(f.targetRouterState=d,f)),X(c)}),this.routerResourcesFeature?.setupAndRunResources(a.signal)??(c=>c),ry(()=>this.afterPreactivation()),ht(()=>{let{currentSnapshot:c,targetSnapshot:l}=i,d=this.createViewTransition?.(this.environmentInjector,c.root,l.root);return d?ot(d).pipe(ge(()=>i)):X(i)}),Ot(1),ht(c=>{r=!1,this.events.next(new Rs);let l=i.beforeActivateHandler.deferredHandle;return l?ot(l.then(()=>c)):X(c)}),Bt(c=>{new Jv(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),c.newlyCreatedRoutes?.clear(),s()&&(Tx(c.targetRouterState),o=!0,this.currentNavigation.update(l=>(l.abort=M2,l)),this.lastSuccessfulNavigation.set(Se(this.currentNavigation)),this.events.next(new Mi(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0))}),Ee(px(a.signal).pipe(je(()=>!o&&r),Bt(()=>{this.cancelNavigationTransition(i,a.signal.reason+"",nn.Aborted)}))),Bt({complete:()=>{o=!0}}),Ee(this.transitionAbortWithErrorSubject.pipe(Bt(c=>{throw c}))),qo(()=>{a.abort(),o||this.cancelNavigationTransition(i,"",nn.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Jr(c=>{if(o=!0,zE(i),this.destroyed)return i.resolve(!1),vt;if(hx(c))this.events.next(new ci(i.id,this.urlSerializer.serialize(i.extractedUrl),c.message,c.cancellationCode)),FL(c)?this.events.next(new Os(c.url,c.navigationBehaviorOptions)):i.resolve(!1);else{let l=new Na(i.id,this.urlSerializer.serialize(i.extractedUrl),c,i.targetSnapshot??void 0);try{let d=Ft(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(d instanceof Fs){let{message:f,cancellationCode:p}=Lh(this.urlSerializer,d);this.events.next(new ci(i.id,this.urlSerializer.serialize(i.extractedUrl),f,p)),this.events.next(new Os(d.redirectTo,d.navigationBehaviorOptions))}else throw this.events.next(l),c}catch(d){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(d)}}return vt}))}))}cancelNavigationTransition(e,i,r){zE(e);let o=new ci(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Se(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function I2(t){return t!==Ql}function zE(t){for(let n of t.newlyCreatedRoutes??[])n._localInjector?.destroy(),n._localInjector=void 0;Tx(t.targetRouterState)}function Tx(t){if(!t)return;let n=e=>{e.value.pending?.set(!1),e.children.forEach(n)};n(t._root)}var kx=new w("");var Ax=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:()=>u(N2)})}return t})(),jh=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},N2=(()=>{class t extends jh{static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),zh=(()=>{class t{urlSerializer=u(Vs);options=u(Us,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=u(ai);urlHandlingStrategy=u(Hh);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new hn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,a=r??o;return a instanceof hn?this.urlSerializer.serialize(a):a}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=sx(null,u(qe));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:()=>u(T2)})}return t})(),T2=(()=>{class t extends zh{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof Ia?this.updateStateMemento():e instanceof Or?this.commitTransition(i):e instanceof td?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Rs?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof ci&&!ax(e)?this.restoreHistory(i):e instanceof Na?this.restoreHistory(i,!0):e instanceof Mi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:a,state:s}=r;if(this.location.isCurrentPathEqualTo(e)||a){let c=this.browserPageId,l=C(C({},s),this.generateNgRouterState(o,c,i));this.location.replaceState(e,"",l)}else{let c=C(C({},s),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",c)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?C({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):C({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function fy(t,n){t.events.pipe(je(e=>e instanceof Mi||e instanceof ci||e instanceof Na||e instanceof Or),ge(e=>e instanceof Mi||e instanceof Or?0:(e instanceof ci?e.code===nn.Redirect||e.code===nn.SupersededByNewNavigation:!1)?2:1),je(e=>e!==2),Ot(1)).subscribe(()=>{n()})}var qt=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=u(Xf);stateManager=u(zh);options=u(Us,{optional:!0})||{};pendingTasks=u(Cr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=u(Nx);urlSerializer=u(Vs);location=u(ai);urlHandlingStrategy=u(Hh);injector=u(qe);_events=new N;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=u(Ax);injectorCleanup=u(kx,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=u(dd,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!u(Bh,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new ue;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Se(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof ci&&i.code!==nn.Redirect&&i.code!==nn.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Mi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Os){let a=i.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=C({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||I2(r.source)},a);this.scheduleNavigation(s,Ql,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}TL(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Ql,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let a=r?.navigationId?r:null,s=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=re(C({},o),{browserUrl:e})),r){let l=C({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(s);this.scheduleNavigation(c,i,a,o).catch(l=>{this.disposed||this.injector.get(Ln)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Se(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(dy),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:a,queryParamsHandling:s,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:a,d=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":d=C(C({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let f;try{let p=r?r.snapshot:this.routerState.snapshot.root;f=nx(p)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return ix(f,e,d,l??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=yo(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Ql,null,i)}navigate(e,i={skipLocationChange:!1}){return k2(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(Gi(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=C({},oy):i===!1?r=C({},As):r=C(C({},As),i),yo(e))return Gv(this.currentUrlTree,e,r);let o=this.parseUrl(e);return Gv(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,a){if(this.disposed)return Promise.resolve(!1);let s,c,l;a?(s=a.resolve,c=a.reject,l=a.promise):l=new Promise((f,p)=>{s=f,c=p});let d=this.pendingTasks.add();return fy(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(d))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:s,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function k2(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new k(4008,!1)}var R2=(()=>{class t{router=u(qt);stateManager=u(zh);fragment=S("");queryParams=S({});path=S("");serializer=u(Vs);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof Mi&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new hn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),Ht=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=u(new ri("href"),{optional:!0});reactiveHref=nh(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Se(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return Se(this._target)}_target=S(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return Se(this._queryParams)}_queryParams=S(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return Se(this._fragment)}_fragment=S(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return Se(this._queryParamsHandling)}_queryParamsHandling=S(void 0);set state(e){this._state.set(e)}get state(){return Se(this._state)}_state=S(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return Se(this._info)}_info=S(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return Se(this._relativeTo)}_relativeTo=S(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return Se(this._preserveFragment)}_preserveFragment=S(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return Se(this._skipLocationChange)}_skipLocationChange=S(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return Se(this._replaceUrl)}_replaceUrl=S(!1);browserUrl=Sa(void 0);isAnchorElement;onChanges=new N;applicationErrorHandler=u(Ln);options=u(Us,{optional:!0});reactiveRouterState=u(R2);constructor(e,i,r,o,a,s){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=a,this.locationStrategy=s;let c=a.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=S(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(yo(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,a){let s=this._urlTree();if(s===null||this.isAnchorElement&&(e!==0||i||r||o||a||typeof this.target=="string"&&this.target!="_self"))return!0;let c=this.browserUrl(),l=C({skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info},c!==void 0&&{browserUrl:c});return this.router.navigateByUrl(s,l)?.catch(d=>{this.applicationErrorHandler(d)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=ct(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:yo(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return Se(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(ie(qt),ie(mn),wl("tabindex"),ie(Te),ie(U),ie(Ms))};static \u0275dir=P({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&I("click",function(a){return r.onClick(a.button,a.ctrlKey,a.shiftKey,a.altKey,a.metaKey)}),i&2&&ee("href",r.reactiveHref(),O_)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",j],skipLocationChange:[2,"skipLocationChange","skipLocationChange",j],replaceUrl:[2,"replaceUrl","replaceUrl",j],browserUrl:[1,"browserUrl"],routerLink:"routerLink"},features:[Pe]})}return t})(),hy=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new A;link=u(Ht,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(a=>{a instanceof Mi&&this.update()})}ngAfterContentInit(){X(this.links.changes,X(null)).pipe(Qr()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=ot(e).pipe(Qr()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){if(e==null){this.classes=[];return}let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||this.routerLinkActiveOptions===null&&!this._isActive||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=this.routerLinkActiveOptions;if(i===null)return()=>!1;let r;return i===void 0?r=C({},As):O2(i)?r=i:i.exact??!1?r=C({},oy):r=C({},As),o=>{let a=o.urlTree;return a?Se(ay(a,e,r)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(ie(qt),ie(U),ie(Te),ie(De))};static \u0275dir=P({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&Wt(o,Ht,5),i&2){let a;K(a=Z())&&(r.links=a)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[Pe]})}return t})();function O2(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var P2=new w("");function my(t,...n){return Wi([{provide:dd,multi:!0,useValue:t},{provide:mn,useFactory:F2},{provide:Nl,multi:!0,useFactory:L2},n.map(e=>e.\u0275providers)])}function F2(){return u(qt).routerState.root}function L2(){let t=u(te);return n=>{let e=t.get(Cn);if(n!==e.components[0])return;let i=t.get(qt),r=t.get(V2);t.get(j2)===1&&i.initialNavigation(),t.get(B2,null,{optional:!0})?.setUpPreloading(),t.get(P2,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var V2=new w("",{factory:()=>new N}),j2=new w("",{factory:()=>1});var B2=new w("");function ka(t){return t.buttons===0||t.detail===0}function Aa(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var py;function Rx(){if(py==null){let t=typeof document<"u"?document.head:null;py=!!(t&&(t.createShadowRoot||t.attachShadow))}return py}function gy(t){if(Rx()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Hs(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Yt(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var _y;try{_y=typeof Intl<"u"&&Intl.v8BreakIterator}catch{_y=!1}var xe=(()=>{class t{_platformId=u(la);isBrowser=this._platformId?_E(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||_y)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var ud;function Ox(){if(ud==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>ud=!0}))}finally{ud=ud||!1}return ud}function zs(t){return Ox()?t:!!t.capture}function $s(t,n=0){return Px(t)?Number(t):arguments.length===2?n:0}function Px(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function zn(t){return t instanceof U?t.nativeElement:t}var Fx=new w("cdk-input-modality-detector-options"),Lx={ignoreKeys:[18,17,224,91,16]},Vx=650,vy={passive:!0,capture:!0},jx=(()=>{class t{_platform=u(xe);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new At(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Yt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<Vx||(this._modality.next(ka(e)?"keyboard":"mouse"),this._mostRecentTarget=Yt(e))};_onTouchstart=e=>{if(Aa(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Yt(e)};constructor(){let e=u(H),i=u($),r=u(Fx,{optional:!0});if(this._options=C(C({},Lx),r),this.modalityDetected=this._modality.pipe($c(1)),this.modalityChanged=this.modalityDetected.pipe(Ru()),this._platform.isBrowser){let o=u(Mt).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,vy),o.listen(i,"mousedown",this._onMousedown,vy),o.listen(i,"touchstart",this._onTouchstart,vy)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),fd=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(fd||{}),Bx=new w("cdk-focus-monitor-default-options"),Gh=zs({passive:!0,capture:!0}),Ii=(()=>{class t{_ngZone=u(H);_platform=u(xe);_inputModalityDetector=u(jx);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u($);_stopInputModalityDetector=new N;constructor(){let e=u(Bx,{optional:!0});this._detectionMode=e?.detectionMode||fd.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Yt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=zn(e);if(!this._platform.isBrowser||r.nodeType!==1)return X();let o=gy(r)||this._document,a=this._elementInfo.get(r);if(a)return i&&(a.checkChildren=!0),a.subject;let s={checkChildren:i,subject:new N,rootNode:o};return this._elementInfo.set(r,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=zn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=zn(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,c])=>this._originChanged(s,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===fd.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===fd.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?Vx:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Yt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Gh),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Gh)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ee(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Gh),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Gh),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),Wh=(()=>{class t{_elementRef=u(U);_focusMonitor=u(Ii);_monitorSubscription;_focusOrigin=null;cdkFocusChange=new A;get focusOrigin(){return this._focusOrigin}ngAfterViewInit(){let e=this._elementRef.nativeElement;this._monitorSubscription=this._focusMonitor.monitor(e,e.nodeType===1&&e.hasAttribute("cdkMonitorSubtreeFocus")).subscribe(i=>{this._focusOrigin=i,this.cdkFocusChange.emit(i)})}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef),this._monitorSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","cdkMonitorElementFocus",""],["","cdkMonitorSubtreeFocus",""]],outputs:{cdkFocusChange:"cdkFocusChange"},exportAs:["cdkMonitorFocus"]})}return t})();var qh=new WeakMap,Ge=(()=>{class t{_appRef;_injector=u(te);_environmentInjector=u(qe);load(e){let i=this._appRef=this._appRef||this._injector.get(Cn),r=qh.get(i);r||(r={loaders:new Set,refs:[]},qh.set(i,r),i.onDestroy(()=>{qh.get(i)?.refs.forEach(o=>o.destroy()),qh.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(rh(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var Dn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})(),Yh;function U2(){if(Yh===void 0&&(Yh=null,typeof window<"u")){let t=window;if(t.trustedTypes!==void 0)try{Yh=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n})}catch(n){console.error(n)}}return Yh}function Ra(t){return U2()?.createHTML(t)||t}function Ux(t,n,e){let i=e.sanitize(Oe.HTML,n);t.innerHTML=Ra(i||"")}function Gs(t){return Array.isArray(t)?t:[t]}var Hx=new Set,Oa,Ws=(()=>{class t{_platform=u(xe);_nonce=u(co,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):z2}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&H2(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function H2(t,n){if(!Hx.has(t))try{Oa||(Oa=document.createElement("style"),n&&Oa.setAttribute("nonce",n),Oa.setAttribute("type","text/css"),document.head.appendChild(Oa)),Oa.sheet&&(Oa.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),Hx.add(t))}catch(e){console.error(e)}}function z2(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var hd=(()=>{class t{_mediaMatcher=u(Ws);_zone=u(H);_queries=new Map;_destroySubject=new N;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return zx(Gs(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=zx(Gs(e)).map(a=>this._registerQuery(a).observable),o=Bc(r);return o=Xr(o.pipe(Ot(1)),o.pipe($c(1),Hc(0))),o.pipe(ge(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:c,query:l})=>{s.matches=s.matches||c,s.breakpoints[l]=c}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new he(a=>{let s=c=>this._zone.run(()=>a.next(c));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(dt(i),ge(({matches:a})=>({query:e,matches:a})),Ee(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function zx(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var $2=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var Kh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({providers:[$2]})}return t})();var Qh=(()=>{class t{_platform=u(xe);isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return W2(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=G2(eV(e));if(i&&($x(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=$x(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!X2(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return J2(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function G2(t){try{return t.frameElement}catch{return null}}function W2(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function q2(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function Y2(t){return Z2(t)&&t.type=="hidden"}function K2(t){return Q2(t)&&t.hasAttribute("href")}function Z2(t){return t.nodeName.toLowerCase()=="input"}function Q2(t){return t.nodeName.toLowerCase()=="a"}function qx(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function $x(t){if(!qx(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function X2(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function J2(t){return Y2(t)?!1:q2(t)||K2(t)||t.hasAttribute("contenteditable")||qx(t)}function eV(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var Zh=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>{!this.focusLastTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};endAnchorListener=()=>{!this.focusFirstTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,a){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){mt(n,{injector:this._injector})}},Yx=(()=>{class t{_checker=u(Qh);_ngZone=u(H);_document=u($);_injector=u(te);constructor(){u(Ge).load(Dn)}create(e,i=!1){return new Zh(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),by=(()=>{class t{_elementRef=u(U);_focusTrapFactory=u(Yx);focusTrap=void 0;_previouslyFocusedElement=null;get enabled(){return this.focusTrap?.enabled||!1}set enabled(e){this.focusTrap&&(this.focusTrap.enabled=e)}autoCapture=!1;constructor(){u(xe).isBrowser&&(this.focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement,!0))}ngOnDestroy(){this.focusTrap?.destroy(),this._previouslyFocusedElement&&(this._previouslyFocusedElement.focus(),this._previouslyFocusedElement=null)}ngAfterContentInit(){this.focusTrap?.attachAnchors(),this.autoCapture&&this._captureFocus()}ngDoCheck(){this.focusTrap&&!this.focusTrap.hasAttached()&&this.focusTrap.attachAnchors()}ngOnChanges(e){let i=e.autoCapture;i&&!i.firstChange&&this.autoCapture&&this.focusTrap?.hasAttached()&&this._captureFocus()}_captureFocus(){this._previouslyFocusedElement=Hs(),this.focusTrap?.focusInitialElementWhenReady()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","cdkTrapFocus",""]],inputs:{enabled:[2,"cdkTrapFocus","enabled",j],autoCapture:[2,"cdkTrapFocusAutoCapture","autoCapture",j]},exportAs:["cdkTrapFocus"],features:[Pe]})}return t})(),Kx=new w("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),Zx=new w("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),tV=0,md=(()=>{class t{_ngZone=u(H);_defaultOptions=u(Zx,{optional:!0});_liveElement;_document=u($);_sanitizer=u(ql);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=u(Kx,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,a;return i.length===1&&typeof i[0]=="number"?a=i[0]:[o,a]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),a==null&&r&&(a=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:Ux(this._liveElement,e,this._sanitizer),typeof a=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),a)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${tV++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var bo=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(bo||{}),Gx="cdk-high-contrast-black-on-white",Wx="cdk-high-contrast-white-on-black",yy="cdk-high-contrast-active",Qx=(()=>{class t{_platform=u(xe);_hasCheckedHighContrastMode=!1;_document=u($);_breakpointSubscription;constructor(){this._breakpointSubscription=u(hd).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return bo.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return bo.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return bo.BLACK_ON_WHITE}return bo.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(yy,Gx,Wx),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===bo.BLACK_ON_WHITE?e.add(yy,Gx):i===bo.WHITE_ON_BLACK&&e.add(yy,Wx)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),pd=(()=>{class t{constructor(){u(Qx)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Kh]})}return t})();var nV=200,Xh=class{_letterKeyStream=new N;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new N;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:nV;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Bt(e=>this._pressedLetters.push(e)),Hc(n),je(()=>this._pressedLetters.length>0),ge(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function Ct(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var qs=class{_items;_activeItemIndex=S(-1);_activeItem=S(null);_wrap=!1;_typeaheadSubscription=ue.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Si?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):bn(n)&&(this._effectRef=Vn(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new N;change=new N;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Xh(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,a=this._getItemsArray().length;this._setActiveItemByIndex(o<a?o:a-1,-1);break}else return;default:(r||Ct(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return bn(this._items)?this._items():this._items instanceof Si?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var vd=class extends qs{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Pa=class extends qs{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var Jx=new Map,Ye=class t{_appId=u(wr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=Jx.get(n);return i===void 0?i=0:i++,Jx.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})};var tM=" ";function iV(t,n,e){let i=nm(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(tM)))}function rV(t,n,e){let i=nm(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(tM)):t.removeAttribute(n)}function nm(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var nM="cdk-describedby-message",tm="cdk-describedby-host",wy=0,im=(()=>{class t{_platform=u(xe);_document=u($);_messageRegistry=new Map;_messagesContainer=null;_id=`${wy++}`;constructor(){u(Ge).load(Dn),this._id=u(wr)+"-"+wy++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=Cy(i,r);typeof i!="string"?(eM(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=Cy(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let a=this._messageRegistry.get(o);a&&a.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${tm}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(tm);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");eM(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(Cy(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=nm(e,"aria-describedby").filter(r=>r.indexOf(nM)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);iV(e,"aria-describedby",r.messageElement.id),e.setAttribute(tm,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,rV(e,"aria-describedby",r.messageElement.id),e.removeAttribute(tm)}_isElementDescribedByMessage(e,i){let r=nm(e,"aria-describedby"),o=this._messageRegistry.get(i),a=o&&o.messageElement.id;return!!a&&r.indexOf(a)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function Cy(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function eM(t,n){t.id||(t.id=`${nM}-${n}-${wy++}`)}var Fa;function iM(){if(Fa==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Fa=!1,Fa;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Fa=!0;else{let t=Element.prototype.scrollTo;t?Fa=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Fa=!1}}return Fa}function Sy(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Ys,rM=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Dy(){if(Ys)return Ys;if(typeof document!="object"||!document)return Ys=new Set(rM),Ys;let t=document.createElement("input");return Ys=new Set(rM.filter(n=>(t.setAttribute("type",n),t.type===n))),Ys}var oM={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var oV=new w("MATERIAL_ANIMATIONS"),aM=null;function aV(){return u(oV,{optional:!0})?.animationsDisabled||u(cl,{optional:!0})==="NoopAnimations"?"di-disabled":(aM??=u(Ws).matchMedia("(prefers-reduced-motion)").matches,aM?"reduced-motion":"enabled")}function We(){return aV()!=="enabled"}function wt(t){return t==null?"":typeof t=="string"?t:`${t}px`}function Pr(t){return t!=null&&`${t}`!="false"}function sM(t,n=/\s+/){let e=[];if(t!=null){let i=Array.isArray(t)?t:`${t}`.split(n);for(let r of i){let o=`${r}`.trim();o&&e.push(o)}}return e}var ui=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(ui||{}),Ey=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=ui.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},cM=zs({passive:!0,capture:!0}),xy=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let a=o.get(i);a?a.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,cM)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,cM)))}_delegateEventHandler=n=>{let e=Yt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},yd={enterDuration:225,exitDuration:150},sV=800,lM=zs({passive:!0,capture:!0}),dM=["mousedown","touchstart"],uM=["mouseup","mouseleave","touchend","touchcancel"],cV=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--%NS%mat-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),bd=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new xy;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=zn(i)),o&&o.get(Ge).load(cV)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=C(C({},yd),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let a=i.radius||lV(n,e,r),s=n-r.left,c=e-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${s-a}px`,d.style.top=`${c-a}px`,d.style.height=`${a*2}px`,d.style.width=`${a*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let f=window.getComputedStyle(d),p=f.transitionProperty,_=f.transitionDuration,b=p==="none"||_==="0s"||_==="0s, 0s"||r.width===0&&r.height===0,M=new Ey(this,d,i,b);d.style.transform="scale3d(1, 1, 1)",M.state=ui.FADING_IN,i.persistent||(this._mostRecentTransientRipple=M);let x=null;return!b&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let E=()=>{x&&(x.fallbackTimer=null),clearTimeout(W),this._finishRippleTransition(M)},L=()=>this._destroyRipple(M),W=setTimeout(L,l+100);d.addEventListener("transitionend",E),d.addEventListener("transitioncancel",L),x={onTransitionEnd:E,onTransitionCancel:L,fallbackTimer:W}}),this._activeRipples.set(M,x),(b||!l)&&this._finishRippleTransition(M),M}fadeOutRipple(n){if(n.state===ui.FADING_OUT||n.state===ui.HIDDEN)return;let e=n.element,i=C(C({},yd),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=ui.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=zn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,dM.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{uM.forEach(e=>{this._triggerElement.addEventListener(e,this,lM)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===ui.FADING_IN?this._startFadeOutTransition(n):n.state===ui.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=ui.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=ui.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=ka(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+sV;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Aa(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===ui.VISIBLE||n.config.terminateOnPointerUp&&n.state===ui.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(dM.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(uM.forEach(e=>n.removeEventListener(e,this,lM)),this._pointerUpEventsRegistered=!1))}};function lV(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Cd=new w("mat-ripple-global-options"),Ks=(()=>{class t{_elementRef=u(U);_animationsDisabled=We();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=u(H),i=u(xe),r=u(Cd,{optional:!0}),o=u(te);this._globalOptions=r||{},this._rippleRenderer=new bd(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:C(C(C({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,C(C({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,C(C({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var dV={capture:!0},uV=["focus","mousedown","mouseenter","touchstart"],My="mat-ripple-loader-uninitialized",Iy="mat-ripple-loader-class-name",fM="mat-ripple-loader-centered",rm="mat-ripple-loader-disabled",om=(()=>{class t{_document=u($);_animationsDisabled=We();_globalRippleOptions=u(Cd,{optional:!0});_platform=u(xe);_ngZone=u(H);_injector=u(te);_eventCleanups;_hosts=new Map;constructor(){let e=u(Mt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>uV.map(i=>e.listen(this._document,i,this._onInteraction,dV)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(My,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Iy))&&e.setAttribute(Iy,i.className||""),i.centered&&e.setAttribute(fM,""),i.disabled&&e.setAttribute(rm,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(rm,""):e.removeAttribute(rm)}_onInteraction=e=>{let i=Yt(e);if(i instanceof HTMLElement){let r=i.closest(`[${My}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Iy)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??yd.enterDuration,a=this._animationsDisabled?0:r?.animation?.exitDuration??yd.exitDuration,s={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(rm),rippleConfig:{centered:e.hasAttribute(fM),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},c=new bd(s,this._ngZone,i,this._platform,this._injector),l=!s.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:c,hasSetUpEvents:l}),e.removeAttribute(My)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var $n=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--%NS%mat-focus-indicator-display, none);
  border-width: var(--%NS%mat-focus-indicator-border-width, 3px);
  border-style: var(--%NS%mat-focus-indicator-border-style, solid);
  border-color: var(--%NS%mat-focus-indicator-border-color, transparent);
  border-radius: var(--%NS%mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --%NS%mat-focus-indicator-display: block;
    --%NS%mat-focus-indicator-fallback-border-style: none;
  }
}
`],encapsulation:2})}return t})();var fV=["*",[["","progressIndicator",""]]],hV=["*","[progressIndicator]"];function mV(t,n){t&1&&(Ve(0,"div",1),se(1,1),$e())}var pV=new w("MAT_BUTTON_CONFIG");function hM(t){return t==null?void 0:ir(t)}var Ny=(()=>{class t{_elementRef=u(U);_ngZone=u(H);_animationsDisabled=We();_config=u(pV,{optional:!0});_focusMonitor=u(Ii);_cleanupClick;_renderer=u(Te);_rippleLoader=u(om);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=Sa(!1,{transform:j});constructor(){u(Ge).load($n);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(ee("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),bt(r.color?"mat-"+r.color:""),z("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",j],disabled:[2,"disabled","disabled",j],ariaDisabled:[2,"aria-disabled","ariaDisabled",j],disabledInteractive:[2,"disabledInteractive","disabledInteractive",j],tabIndex:[2,"tabIndex","tabIndex",hM],_tabindex:[2,"tabindex","_tabindex",hM],showProgress:[1,"showProgress"]}})}return t})(),or=(()=>{class t extends Ny{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[Be],ngContentSelectors:hV,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(ke(fV),wn(0,"span",0),se(1),R(2,mV,2,0,"div",1),wn(3,"span",2)(4,"span",3)),i&2&&(g(2),O(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--%NS%mat-icon-button-state-layer-size, 40px);
  height: var(--%NS%mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--%NS%mat-icon-button-state-layer-size, 40px) - var(--%NS%mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--%NS%mat-icon-button-icon-size, 24px);
  color: var(--%NS%mat-icon-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--%NS%mat-icon-button-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface-variant) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-icon-button-touch-target-size, 48px);
  display: var(--%NS%mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--%NS%mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--%NS%mat-icon-button-icon-size, 24px);
  height: var(--%NS%mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();var gV=new w("cdk-dir-doc",{providedIn:"root",factory:()=>u($)}),_V=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function mM(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?_V.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Tt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=S("ltr");change=new A;constructor(){let e=u(gV,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(mM(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var Ne=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({})}return t})();var Co=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Ne]})}return t})();var vV=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],yV=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function bV(t,n){t&1&&(Ve(0,"div",2),se(1,3),$e())}var pM=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),St=(()=>{class t extends Ny{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=CV(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?pM.get(this._appearance):null,o=pM.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Be],ngContentSelectors:yV,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(ke(vV),wn(0,"span",0),se(1),Ve(2,"span",1),se(3,1),$e(),se(4,2),R(5,bV,2,0,"div",2),wn(6,"span",3)(7,"span",4)),i&2&&(z("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),g(5),O(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--%NS%mat-button-text-horizontal-padding, 12px);
  height: var(--%NS%mat-button-text-container-height, 40px);
  font-family: var(--%NS%mat-button-text-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-text-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-text-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-text-label-text-transform);
  font-weight: var(--%NS%mat-button-text-label-text-weight, var(--%NS%mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-text-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--%NS%mat-button-text-label-text-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--%NS%mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-text-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-text-touch-target-size, 48px);
  display: var(--%NS%mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-filled-container-height, 40px);
  font-family: var(--%NS%mat-button-filled-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-filled-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-filled-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-filled-label-text-transform);
  font-weight: var(--%NS%mat-button-filled-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-filled-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-state-layer-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-filled-touch-target-size, 48px);
  display: var(--%NS%mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--%NS%mat-button-filled-label-text-color, var(--%NS%mat-sys-on-primary));
  background-color: var(--%NS%mat-button-filled-container-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-filled-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --%NS%mat-progress-spinner-active-indicator-color: var(--%NS%mat-button-filled-progress-active-indicator-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--%NS%mat-button-protected-container-elevation-shadow, var(--%NS%mat-sys-level1));
  height: var(--%NS%mat-button-protected-container-height, 40px);
  font-family: var(--%NS%mat-button-protected-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-protected-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-protected-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-protected-label-text-transform);
  font-weight: var(--%NS%mat-button-protected-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-protected-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-protected-touch-target-size, 48px);
  display: var(--%NS%mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--%NS%mat-button-protected-label-text-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-button-protected-container-color, var(--%NS%mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-protected-container-shape, var(--%NS%mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--%NS%mat-button-protected-hover-container-elevation-shadow, var(--%NS%mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--%NS%mat-button-protected-focus-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--%NS%mat-button-protected-pressed-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-protected-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--%NS%mat-button-protected-disabled-container-elevation-shadow, var(--%NS%mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-outlined-container-height, 40px);
  font-family: var(--%NS%mat-button-outlined-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-outlined-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-outlined-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-outlined-label-text-transform);
  font-weight: var(--%NS%mat-button-outlined-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  border-radius: var(--%NS%mat-button-outlined-container-shape, var(--%NS%mat-sys-corner-full));
  border-width: var(--%NS%mat-button-outlined-outline-width, 1px);
  padding: 0 var(--%NS%mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-outlined-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-outlined-touch-target-size, 48px);
  display: var(--%NS%mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--%NS%mat-button-outlined-label-text-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-button-outlined-outline-color, var(--%NS%mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: var(--%NS%mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-tonal-container-height, 40px);
  font-family: var(--%NS%mat-button-tonal-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-tonal-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-tonal-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-tonal-label-text-transform);
  font-weight: var(--%NS%mat-button-tonal-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--%NS%mat-button-tonal-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-tonal-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-tonal-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-tonal-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-secondary-container) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-tonal-touch-target-size, 48px);
  display: var(--%NS%mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();function CV(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Vt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Co,Ne]})}return t})();var wV=["*"];var SV=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],DV=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],EV=new w("MAT_CARD_CONFIG"),En=(()=>{class t{appearance;constructor(){let e=u(EV,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&z("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:wV,decls:1,vars:0,template:function(i,r){i&1&&(ke(),se(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-elevated-container-elevation, var(--%NS%mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--%NS%mat-card-outlined-container-color, var(--%NS%mat-sys-surface));
  border-radius: var(--%NS%mat-card-outlined-container-shape, var(--%NS%mat-sys-corner-medium));
  border-width: var(--%NS%mat-card-outlined-outline-width, 1px);
  border-color: var(--%NS%mat-card-outlined-outline-color, var(--%NS%mat-sys-outline-variant));
  box-shadow: var(--%NS%mat-card-outlined-container-elevation, var(--%NS%mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--%NS%mat-card-filled-container-color, var(--%NS%mat-sys-surface-container-highest));
  border-radius: var(--%NS%mat-card-filled-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-filled-container-elevation, var(--%NS%mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--%NS%mat-card-title-text-font, var(--%NS%mat-sys-title-large-font));
  line-height: var(--%NS%mat-card-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-size: var(--%NS%mat-card-title-text-size, var(--%NS%mat-sys-title-large-size));
  letter-spacing: var(--%NS%mat-card-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  font-weight: var(--%NS%mat-card-title-text-weight, var(--%NS%mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--%NS%mat-card-subtitle-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-card-subtitle-text-font, var(--%NS%mat-sys-title-medium-font));
  line-height: var(--%NS%mat-card-subtitle-text-line-height, var(--%NS%mat-sys-title-medium-line-height));
  font-size: var(--%NS%mat-card-subtitle-text-size, var(--%NS%mat-sys-title-medium-size));
  letter-spacing: var(--%NS%mat-card-subtitle-text-tracking, var(--%NS%mat-sys-title-medium-tracking));
  font-weight: var(--%NS%mat-card-subtitle-text-weight, var(--%NS%mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})}return t})(),gM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var am=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})(),_M=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-card-subtitle"],["","mat-card-subtitle",""],["","matCardSubtitle",""]],hostAttrs:[1,"mat-mdc-card-subtitle"]})}return t})(),vM=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})(),yM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:DV,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(ke(SV),se(0),Ve(1,"div",0),se(2,1),$e(),se(3,2))},encapsulation:2})}return t})();var sm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","mat-card-image",""],["","matCardImage",""]],hostAttrs:[1,"mat-mdc-card-image","mdc-card__media"]})}return t})();var xn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Ne]})}return t})();function bM(t){return Error(`Unable to find icon with the name "${t}"`)}function xV(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function CM(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function wM(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Fr=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},DM=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Fr(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let a=this._sanitizer.sanitize(Oe.HTML,r);if(!a)throw wM(r);let s=Ra(a);return this._addSvgIconConfig(e,i,new Fr("",s,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Fr(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(Oe.HTML,i);if(!o)throw wM(i);let a=Ra(o);return this._addSvgIconSetConfig(e,new Fr("",a,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(Oe.RESOURCE_URL,e);if(!i)throw CM(e);let r=this._cachedIconsByUrl.get(i);return r?X(cm(r)):this._loadSvgIconFromConfig(new Fr(e,null)).pipe(Bt(o=>this._cachedIconsByUrl.set(i,o)),ge(o=>cm(o)))}getNamedSvgIcon(e,i=""){let r=SM(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let a=this._iconSetConfigs.get(i);return a?this._getSvgFromIconSetConfigs(e,a):Vc(bM(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?X(cm(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ge(i=>cm(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return X(r);let o=i.filter(a=>!a.svgText).map(a=>this._loadSvgIconSetFromConfig(a).pipe(Jr(s=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(Oe.RESOURCE_URL,a.url)} failed: ${s.message}`;return this._errorHandler.handleError(new Error(l)),X(null)})));return Uc(o).pipe(ge(()=>{let a=this._extractIconWithNameFromAnySet(e,i);if(!a)throw bM(e);return a}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let a=this._svgElementFromConfig(o),s=this._extractSvgIconFromSet(a,e,o.options);if(s)return s}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(Bt(i=>e.svgText=i),ge(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?X(null):this._fetchIcon(e).pipe(Bt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let a=o.cloneNode(!0);if(a.removeAttribute("id"),a.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(a,r);if(a.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(a),r);let s=this._svgElementFromString(Ra("<svg></svg>"));return s.appendChild(a),this._setSvgAttributes(s,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(Ra("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:a,value:s}=r[o];a!=="id"&&i.setAttribute(a,s)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw xV();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let a=this._sanitizer.sanitize(Oe.RESOURCE_URL,i);if(!a)throw CM(i);let s=this._inProgressUrlFetches.get(a);if(s)return s;let c=this._httpClient.get(a,{responseType:"text",withCredentials:o}).pipe(ge(l=>Ra(l)),qo(()=>this._inProgressUrlFetches.delete(a)),zc());return this._inProgressUrlFetches.set(a,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(SM(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return MV(o)?new Fr(o.url,null,o.options):new Fr(o,null)}}static \u0275fac=function(i){return new(i||t)(J(Vv,8),J(ql),J($,8),J(ln))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function cm(t){return t.cloneNode(!0)}function SM(t,n){return t+":"+n}function MV(t){return!!(t.url&&t.options)}var IV=["*"],NV=new w("MAT_ICON_DEFAULT_OPTIONS"),TV=new w("mat-icon-location",{providedIn:"root",factory:()=>{let t=u($),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),EM=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],kV=EM.map(t=>`[${t}]`).join(", "),AV=/^url\(['"]?#(.*?)['"]?\)$/,Mn=(()=>{class t{_elementRef=u(U);_iconRegistry=u(DM);_location=u(TV);_errorHandler=u(ln);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=ue.EMPTY;constructor(){let e=u(new ri("aria-hidden"),{optional:!0}),i=u(NV,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(a=>{o.setAttribute(a.name,`url('${e}#${a.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(kV),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)EM.forEach(a=>{let s=i[o],c=s.getAttribute(a),l=c?c.match(AV):null;if(l){let d=r.get(s);d||(d=[],r.set(s,d)),d.push({name:a,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Ot(1)).subscribe(o=>this._setSvgElement(o),o=>{let a=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(a))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(ee("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),bt(r.color?"mat-"+r.color:""),z("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",j],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:IV,decls:1,vars:0,template:function(i,r){i&1&&(ke(),se(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--%NS%mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return t})(),In=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Ne]})}return t})();function wd(t,n){return function(){return t.apply(n,arguments)}}var{toString:RV}=Object.prototype,{getPrototypeOf:Zs}=Object,{iterator:Dd,toStringTag:IM}=Symbol,dm=(({hasOwnProperty:t})=>(n,e)=>t.call(n,e))(Object.prototype),Sd=(t,n)=>{let e=t,i=[];for(;e!=null&&e!==Object.prototype;){if(i.indexOf(e)!==-1)return!1;if(i.push(e),dm(e,n))return!0;e=Zs(e)}return!1},OV=(t,n)=>t!=null&&Sd(t,n)?t[n]:void 0,ky=(t=>n=>{let e=RV.call(n);return t[e]||(t[e]=e.slice(8,-1).toLowerCase())})(Object.create(null)),fi=t=>(t=t.toLowerCase(),n=>ky(n)===t),um=t=>n=>typeof n===t,{isArray:Ba}=Array,Ua=um("undefined");function Qs(t){return t!==null&&!Ua(t)&&t.constructor!==null&&!Ua(t.constructor)&&Nn(t.constructor.isBuffer)&&t.constructor.isBuffer(t)}var NM=fi("ArrayBuffer");function PV(t){let n;return typeof ArrayBuffer<"u"&&ArrayBuffer.isView?n=ArrayBuffer.isView(t):n=t&&t.buffer&&NM(t.buffer),n}var FV=um("string"),Nn=um("function"),TM=um("number"),Xs=t=>t!==null&&typeof t=="object",LV=t=>t===!0||t===!1,lm=t=>{if(!Xs(t))return!1;let n=Zs(t);return(n===null||n===Object.prototype||Zs(n)===null)&&!Sd(t,IM)&&!Sd(t,Dd)},VV=t=>{if(!Xs(t)||Qs(t))return!1;try{return Object.keys(t).length===0&&Object.getPrototypeOf(t)===Object.prototype}catch{return!1}},jV=fi("Date"),BV=fi("File"),UV=t=>!!(t&&typeof t.uri<"u"),HV=t=>t&&typeof t.getParts<"u",zV=fi("Blob"),$V=fi("FileList"),GV=fi("Set"),WV=t=>Xs(t)&&Nn(t.pipe);function qV(){return typeof globalThis<"u"?globalThis:typeof self<"u"?self:typeof window<"u"?window:typeof global<"u"?global:{}}var xM=qV(),MM=typeof xM.FormData<"u"?xM.FormData:void 0,YV=t=>{if(!t)return!1;if(MM&&t instanceof MM)return!0;let n=Zs(t);if(!n||n===Object.prototype||!Nn(t.append))return!1;let e=ky(t);return e==="formdata"||e==="object"&&Nn(t.toString)&&t.toString()==="[object FormData]"},KV=fi("URLSearchParams"),[ZV,QV,XV,JV]=["ReadableStream","Request","Response","Headers"].map(fi),ej=t=>t.trim?t.trim():t.replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g,"");function Ed(t,n,{allOwnKeys:e=!1}={}){if(t===null||typeof t>"u")return;let i,r;if(typeof t!="object"&&(t=[t]),Ba(t))for(i=0,r=t.length;i<r;i++)n.call(null,t[i],i,t);else{if(Qs(t))return;let o=e?Object.getOwnPropertyNames(t):Object.keys(t),a=o.length,s;for(i=0;i<a;i++)s=o[i],n.call(null,t[s],s,t)}}function kM(t,n){if(Qs(t))return null;n=n.toLowerCase();let e=Object.keys(t),i=e.length,r;for(;i-- >0;)if(r=e[i],n===r.toLowerCase())return r;return null}var ja=typeof globalThis<"u"?globalThis:typeof self<"u"?self:typeof window<"u"?window:global,AM=t=>!Ua(t)&&t!==ja;function Ty(...t){let{caseless:n,skipUndefined:e}=AM(this)&&this||{},i={},r=(o,a)=>{if(a==="__proto__"||a==="constructor"||a==="prototype")return;let s=n&&typeof a=="string"&&kM(i,a)||a,c=dm(i,s)?i[s]:void 0;lm(c)&&lm(o)?i[s]=Ty(c,o):lm(o)?i[s]=Ty({},o):Ba(o)?i[s]=o.slice():(!e||!Ua(o))&&(i[s]=o)};for(let o=0,a=t.length;o<a;o++){let s=t[o];if(!s||Qs(s)||(Ed(s,r),typeof s!="object"||Ba(s)))continue;let c=Object.getOwnPropertySymbols(s);for(let l=0;l<c.length;l++){let d=c[l];fj.call(s,d)&&r(s[d],d)}}return i}var tj=(t,n,e,{allOwnKeys:i}={})=>(Ed(n,(r,o)=>{e&&Nn(r)?Object.defineProperty(t,o,{__proto__:null,value:wd(r,e),writable:!0,enumerable:!0,configurable:!0}):Object.defineProperty(t,o,{__proto__:null,value:r,writable:!0,enumerable:!0,configurable:!0})},{allOwnKeys:i}),t),nj=t=>(t.charCodeAt(0)===65279&&(t=t.slice(1)),t),ij=(t,n,e,i)=>{t.prototype=Object.create(n.prototype,i),Object.defineProperty(t.prototype,"constructor",{__proto__:null,value:t,writable:!0,enumerable:!1,configurable:!0}),Object.defineProperty(t,"super",{__proto__:null,value:n.prototype}),e&&Object.assign(t.prototype,e)},rj=(t,n,e,i)=>{let r,o,a,s={};if(n=n||{},t==null)return n;do{for(r=Object.getOwnPropertyNames(t),o=r.length;o-- >0;)a=r[o],(!i||i(a,t,n))&&!s[a]&&(n[a]=t[a],s[a]=!0);t=e!==!1&&Zs(t)}while(t&&(!e||e(t,n))&&t!==Object.prototype);return n},oj=(t,n,e)=>{t=String(t),(e===void 0||e>t.length)&&(e=t.length),e-=n.length;let i=t.indexOf(n,e);return i!==-1&&i===e},aj=t=>{if(!t)return null;if(Ba(t))return t;let n=t.length;if(!TM(n))return null;let e=new Array(n);for(;n-- >0;)e[n]=t[n];return e},sj=(t=>n=>t&&n instanceof t)(typeof Uint8Array<"u"&&Zs(Uint8Array)),cj=(t,n)=>{let i=(t&&t[Dd]).call(t),r;for(;(r=i.next())&&!r.done;){let o=r.value;n.call(t,o[0],o[1])}},lj=(t,n)=>{let e,i=[];for(;(e=t.exec(n))!==null;)i.push(e);return i},dj=fi("HTMLFormElement"),uj=t=>t.toLowerCase().replace(/[-_\s]([a-z\d])(\w*)/g,function(e,i,r){return i.toUpperCase()+r}),{propertyIsEnumerable:fj}=Object.prototype,hj=fi("RegExp"),RM=(t,n)=>{let e=Object.getOwnPropertyDescriptors(t),i={};Ed(e,(r,o)=>{let a;(a=n(r,o,t))!==!1&&(i[o]=a||r)}),Object.defineProperties(t,i)},mj=t=>{RM(t,(n,e)=>{if(Nn(t)&&["arguments","caller","callee"].includes(e))return!1;let i=t[e];if(Nn(i)){if(n.enumerable=!1,"writable"in n){n.writable=!1;return}n.set||(n.set=()=>{throw Error("Can not rewrite read-only method '"+e+"'")})}})},pj=(t,n)=>{let e={},i=r=>{r.forEach(o=>{e[o]=!0})};return Ba(t)?i(t):i(String(t).split(n)),e},gj=()=>{},_j=(t,n)=>t!=null&&Number.isFinite(t=+t)?t:n;function vj(t){return!!(t&&Nn(t.append)&&t[IM]==="FormData"&&t[Dd])}var yj=t=>{let n=new WeakSet,e=i=>{if(Xs(i)){if(n.has(i))return;if(Qs(i))return i;if(!("toJSON"in i)){n.add(i);let r;if(GV(i)){r=[];for(let o of i){let a=e(o);!Ua(a)&&r.push(a)}}else r=Ba(i)?[]:{},Ed(i,(o,a)=>{let s=e(o);!Ua(s)&&(r[a]=s)});return n.delete(i),r}}return i};return e(t)},bj=fi("AsyncFunction"),Cj=t=>t&&(Xs(t)||Nn(t))&&Nn(t.then)&&Nn(t.catch),OM=((t,n)=>t?setImmediate:n?((e,i)=>(ja.addEventListener("message",({source:r,data:o})=>{r===ja&&o===e&&i.length&&i.shift()()},!1),r=>{i.push(r),ja.postMessage(e,"*")}))(`axios@${Math.random()}`,[]):e=>setTimeout(e))(typeof setImmediate=="function",Nn(ja.postMessage)),wj=typeof queueMicrotask<"u"?queueMicrotask.bind(ja):typeof process<"u"&&process.nextTick||OM,PM=t=>t!=null&&Nn(t[Dd]),Sj=t=>t!=null&&Sd(t,Dd)&&PM(t),y={isArray:Ba,isArrayBuffer:NM,isBuffer:Qs,isFormData:YV,isArrayBufferView:PV,isString:FV,isNumber:TM,isBoolean:LV,isObject:Xs,isPlainObject:lm,isEmptyObject:VV,isReadableStream:ZV,isRequest:QV,isResponse:XV,isHeaders:JV,isUndefined:Ua,isDate:jV,isFile:BV,isReactNativeBlob:UV,isReactNative:HV,isBlob:zV,isRegExp:hj,isFunction:Nn,isStream:WV,isURLSearchParams:KV,isTypedArray:sj,isFileList:$V,forEach:Ed,merge:Ty,extend:tj,trim:ej,stripBOM:nj,inherits:ij,toFlatObject:rj,kindOf:ky,kindOfTest:fi,endsWith:oj,toArray:aj,forEachEntry:cj,matchAll:lj,isHTMLForm:dj,hasOwnProperty:dm,hasOwnProp:dm,hasOwnInPrototypeChain:Sd,getSafeProp:OV,reduceDescriptors:RM,freezeMethods:mj,toObjectSet:pj,toCamelCase:uj,noop:gj,toFiniteNumber:_j,findKey:kM,global:ja,isContextDefined:AM,isSpecCompliantForm:vj,toJSONObject:yj,isAsyncFn:bj,isThenable:Cj,setImmediate:OM,asap:wj,isIterable:PM,isSafeIterable:Sj};var Dj=y.toObjectSet(["age","authorization","content-length","content-type","etag","expires","from","host","if-modified-since","if-unmodified-since","last-modified","location","max-forwards","proxy-authorization","referer","retry-after","user-agent"]),FM=t=>{let n={},e,i,r;return t&&t.split(`
`).forEach(function(a){r=a.indexOf(":"),e=a.substring(0,r).trim().toLowerCase(),i=a.substring(r+1).trim();let s=y.hasOwnProp(n,e);!e||s&&y.hasOwnProp(Dj,e)||(e==="set-cookie"?s?n[e].push(i):n[e]=[i]:n[e]=s?n[e]+", "+i:i)}),n};function Ej(t){let n=0,e=t.length;for(;n<e;){let i=t.charCodeAt(n);if(i!==9&&i!==32)break;n+=1}for(;e>n;){let i=t.charCodeAt(e-1);if(i!==9&&i!==32)break;e-=1}return n===0&&e===t.length?t:t.slice(n,e)}var xj=new RegExp("[\\u0000-\\u0008\\u000a-\\u001f\\u007f]+","g"),Mj=new RegExp("[^\\u0009\\u0020-\\u007e\\u0080-\\u00ff]+","g");function Ay(t,n){return y.isArray(t)?t.map(e=>Ay(e,n)):Ej(String(t).replace(n,""))}var LM=t=>Ay(t,xj),Ij=t=>Ay(t,Mj);function fm(t){let n=Object.create(null);return y.forEach(t.toJSON(),(e,i)=>{n[i]=Ij(e)}),n}var VM=Symbol("internals");function xd(t){return t&&String(t).trim().toLowerCase()}function hm(t){return t===!1||t==null?t:y.isArray(t)?t.map(hm):LM(String(t))}function Nj(t){let n=Object.create(null),e=/([^\s,;=]+)\s*(?:=\s*([^,;]+))?/g,i;for(;i=e.exec(t);)n[i[1]]=i[2];return n}var Tj=/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;function Ry(t){let n=0,e=t.length;for(;n<e;){let i=t.charCodeAt(n);if(i!==9&&i!==32)break;n+=1}for(;e>n;){let i=t.charCodeAt(e-1);if(i!==9&&i!==32)break;e-=1}return n===0&&e===t.length?t:t.slice(n,e)}function kj(t){let n=t.length-1;if(n<1||t.charCodeAt(0)!==34||t.charCodeAt(n)!==34)return t;let e="";for(let i=1;i<n;i++){let r=t.charCodeAt(i);if(r===34||r===92&&(i+=1,i>=n))return t;e+=t[i]}return e}function Aj(t){let n=Object.create(null),e=String(t),i=0,r=!1,o=!1;function a(s){let c=Ry(e.slice(i,s)),l=c.indexOf("=");if(l<1)return;let d=Ry(c.slice(0,l));if(!Tj.test(d))return;let f=d.toLowerCase();if(f==="__proto__"||f==="constructor"||f==="prototype")return;let p=Ry(c.slice(l+1));n[f]=kj(p)}for(let s=0;s<e.length;s++){let c=e.charCodeAt(s);r?o?o=!1:c===92?o=!0:c===34&&(r=!1):c===34?r=!0:(c===44||c===59)&&(a(s),i=s+1)}return a(e.length),n}var Rj=t=>/^[-_a-zA-Z0-9^`|~,!#$%&'*+.]+$/.test(t.trim());function Oy(t,n,e,i,r){if(y.isFunction(i))return i.call(this,n,e);if(r&&(n=e),!!y.isString(n)){if(y.isString(i))return n.indexOf(i)!==-1;if(y.isRegExp(i))return i.test(n)}}function Oj(t){return t.trim().toLowerCase().replace(/([a-z\d])(\w*)/g,(n,e,i)=>e.toUpperCase()+i)}function Pj(t,n){let e=y.toCamelCase(" "+n);["get","set","has"].forEach(i=>{Object.defineProperty(t,i+e,{__proto__:null,value:function(r,o,a){return this[i].call(this,n,r,o,a)},configurable:!0})})}var Js=class{constructor(n){n&&this.set(n)}set(n,e,i){let r=this;function o(s,c,l){let d=xd(c);if(!d)return;let f=y.findKey(r,d);(!f||r[f]===void 0||l===!0||l===void 0&&r[f]!==!1)&&(r[f||c]=hm(s))}let a=(s,c)=>y.forEach(s,(l,d)=>o(l,d,c));if(y.isPlainObject(n)||n instanceof this.constructor)a(n,e);else if(y.isString(n)&&(n=n.trim())&&!Rj(n))a(FM(n),e);else if(y.isObject(n)&&y.isSafeIterable(n)){let s=Object.create(null),c,l;for(let d of n){if(!y.isArray(d))throw new TypeError("Object iterator must return a key-value pair");l=d[0],y.hasOwnProp(s,l)?(c=s[l],s[l]=y.isArray(c)?[...c,d[1]]:[c,d[1]]):s[l]=d[1]}a(s,e)}else n!=null&&o(e,n,i);return this}get(n,e){if(n=xd(n),n){let i=y.findKey(this,n);if(i){let r=this[i];if(!e)return r;if(e===!0)return Nj(r);if(y.isFunction(e))return e.call(this,r,i);if(y.isRegExp(e))return e.exec(r);throw new TypeError("parser must be boolean|regexp|function")}}}has(n,e){if(n=xd(n),n){let i=y.findKey(this,n);return!!(i&&this[i]!==void 0&&(!e||Oy(this,this[i],i,e)))}return!1}delete(n,e){let i=this,r=!1;function o(a){if(a=xd(a),a){let s=y.findKey(i,a);s&&(!e||Oy(i,i[s],s,e))&&(delete i[s],r=!0)}}return y.isArray(n)?n.forEach(o):o(n),r}clear(n){let e=Object.keys(this),i=e.length,r=!1;for(;i--;){let o=e[i];(!n||Oy(this,this[o],o,n,!0))&&(delete this[o],r=!0)}return r}normalize(n){let e=this,i={};return y.forEach(this,(r,o)=>{let a=y.findKey(i,o);if(a){e[a]=hm(r),delete e[o];return}let s=n?Oj(o):String(o).trim();s!==o&&delete e[o],e[s]=hm(r),i[s]=!0}),this}concat(...n){return this.constructor.concat(this,...n)}toJSON(n){let e=Object.create(null);return y.forEach(this,(i,r)=>{i!=null&&i!==!1&&(e[r]=n&&y.isArray(i)?i.join(", "):i)}),e}[Symbol.iterator](){return Object.entries(this.toJSON())[Symbol.iterator]()}toString(){return Object.entries(this.toJSON()).map(([n,e])=>n+": "+e).join(`
`)}getSetCookie(){let n=this.get("set-cookie");return y.isArray(n)?n:n==null||n===!1?[]:[n]}get[Symbol.toStringTag](){return"AxiosHeaders"}static from(n){return n instanceof this?n:new this(n)}static parseParameters(n){return Aj(n)}static concat(n,...e){let i=new this(n);return e.forEach(r=>i.set(r)),i}static accessor(n){let i=(this[VM]=this[VM]={accessors:{}}).accessors,r=this.prototype;function o(a){let s=xd(a);i[s]||(Pj(r,a),i[s]=!0)}return y.isArray(n)?n.forEach(o):o(n),this}};Js.accessor(["Content-Type","Content-Length","Accept","Accept-Encoding","User-Agent","Authorization"]);y.reduceDescriptors(Js.prototype,({value:t},n)=>{let e=n[0].toUpperCase()+n.slice(1);return{get:()=>t,set(i){this[e]=i}}});y.freezeMethods(Js);var gt=Js;var Md="[REDACTED ****]";function Fj(t){if(y.hasOwnProp(t,"toJSON"))return!0;let n=Object.getPrototypeOf(t);for(;n&&n!==Object.prototype;){if(y.hasOwnProp(n,"toJSON"))return!0;n=Object.getPrototypeOf(n)}return!1}function Lj(t,n){let e=new Set(n.map(o=>String(o).toLowerCase())),i=[],r=o=>{if(o===null||typeof o!="object"||y.isBuffer(o))return o;if(i.indexOf(o)!==-1)return;o instanceof gt&&(o=o.toJSON()),i.push(o);let a;if(y.isArray(o))a=[],o.forEach((s,c)=>{let l=r(s);y.isUndefined(l)||(a[c]=l)});else{if(!y.isPlainObject(o)&&Fj(o))return i.pop(),o;a=Object.create(null);for(let[s,c]of Object.entries(o)){let l=e.has(s.toLowerCase())?Md:r(c);y.isUndefined(l)||(a[s]=l)}}return i.pop(),a};return r(t)}function jM(t){try{return String(t)}catch{return""}}function Vj(t){return t.errors.map(e=>{try{return e&&e.message?jM(e.message):jM(e)}catch{return""}}).filter(Boolean).join("; ")||t.name||"AggregateError"}var jj=(()=>{class t extends Error{static from(e,i,r,o,a,s){let c=e.message;!c&&y.isArray(e.errors)&&e.errors.length&&(c=Vj(e));let l=new t(c,i||e.code,r,o,a);return Object.defineProperty(l,"cause",{__proto__:null,value:e,writable:!0,enumerable:!1,configurable:!0}),l.name=e.name,e.status!=null&&l.status==null&&(l.status=e.status),s&&Object.assign(l,s),l}constructor(e,i,r,o,a){super(e),Object.defineProperty(this,"message",{__proto__:null,value:e,enumerable:!0,writable:!0,configurable:!0}),this.name="AxiosError",this.isAxiosError=!0,i&&(this.code=i),r&&(this.config=r),o&&(this.request=o),a&&(this.response=a,this.status=a.status)}toJSON(){let e=this.config,i=e&&y.hasOwnProp(e,"redact")?e.redact:void 0,r=y.isArray(i)&&i.length>0?Lj(e,i):y.toJSONObject(e);return{message:this.message,name:this.name,description:this.description,number:this.number,fileName:this.fileName,lineNumber:this.lineNumber,columnNumber:this.columnNumber,stack:this.stack,config:r,code:this.code,status:this.status}}}return t.ERR_BAD_OPTION_VALUE="ERR_BAD_OPTION_VALUE",t.ERR_BAD_OPTION="ERR_BAD_OPTION",t.ECONNABORTED="ECONNABORTED",t.ETIMEDOUT="ETIMEDOUT",t.ECONNREFUSED="ECONNREFUSED",t.ERR_NETWORK="ERR_NETWORK",t.ERR_FR_TOO_MANY_REDIRECTS="ERR_FR_TOO_MANY_REDIRECTS",t.ERR_DEPRECATED="ERR_DEPRECATED",t.ERR_BAD_RESPONSE="ERR_BAD_RESPONSE",t.ERR_BAD_REQUEST="ERR_BAD_REQUEST",t.ERR_CANCELED="ERR_CANCELED",t.ERR_NOT_SUPPORT="ERR_NOT_SUPPORT",t.ERR_INVALID_URL="ERR_INVALID_URL",t.ERR_FORM_DATA_DEPTH_EXCEEDED="ERR_FORM_DATA_DEPTH_EXCEEDED",t})(),B=jj;var So=null;var Ly=100;function Fy(t){return y.isPlainObject(t)||y.isArray(t)}function BM(t){return y.endsWith(t,"[]")?t.slice(0,-2):t}function Py(t,n,e){return t?t.concat(n).map(function(r,o){return r=BM(r),!e&&o?"["+r+"]":r}).join(e?".":""):n}function Bj(t){return y.isArray(t)&&!t.some(Fy)}var Uj=y.toFlatObject(y,{},null,function(n){return/^is[A-Z]/.test(n)});function Hj(t,n,e){if(!y.isObject(t))throw new TypeError("target must be an object");n=n||new(So||FormData),e=y.toFlatObject(e,{metaTokens:!0,dots:!1,indexes:!1},!1,function(L,W){return!y.isUndefined(W[L])});let i=e.metaTokens,r=e.visitor||b,o=e.dots,a=e.indexes,s=e.Blob||typeof Blob<"u"&&Blob,c=e.maxDepth===void 0?Ly:e.maxDepth,l=s&&y.isSpecCompliantForm(n),d=[];if(!y.isFunction(r))throw new TypeError("visitor must be a function");function f(E){if(E===null)return"";if(y.isDate(E))return E.toISOString();if(y.isBoolean(E))return E.toString();if(!l&&y.isBlob(E))throw new B("Blob is not supported. Use a Buffer instead.");if(y.isArrayBuffer(E)||y.isTypedArray(E)){if(l&&typeof s=="function")return new s([E]);if(So&&So.isBufferAvailable())return So.from(E);throw new B("Blob is not supported. Use a Buffer instead.",B.ERR_NOT_SUPPORT)}return E}function p(E){if(E>c)throw new B("Object is too deeply nested ("+E+" levels). Max depth: "+c,B.ERR_FORM_DATA_DEPTH_EXCEEDED)}function _(E,L){if(c===1/0)return JSON.stringify(E);let W=[];return JSON.stringify(E,function(rt,Ze){if(!y.isObject(Ze))return Ze;for(;W.length&&W[W.length-1]!==this;)W.pop();return W.push(Ze),p(L+W.length-1),Ze})}function b(E,L,W){let Me=E;if(y.isReactNative(n)&&y.isReactNativeBlob(E))return n.append(Py(W,L,o),f(E)),!1;if(E&&!W&&typeof E=="object"){if(y.endsWith(L,"{}"))L=i?L:L.slice(0,-2),E=_(E,1);else if(y.isArray(E)&&Bj(E)||(y.isFileList(E)||y.endsWith(L,"[]"))&&(Me=y.toArray(E)))return L=BM(L),Me.forEach(function(Ze,_n){!(y.isUndefined(Ze)||Ze===null)&&n.append(a===!0?Py([L],_n,o):a===null?L:L+"[]",f(Ze))}),!1}return Fy(E)?!0:(n.append(Py(W,L,o),f(E)),!1)}let M=Object.assign(Uj,{defaultVisitor:b,convertValue:f,isVisitable:Fy});function x(E,L,W=0){if(!y.isUndefined(E)){if(p(W),d.indexOf(E)!==-1)throw new Error("Circular reference detected in "+L.join("."));d.push(E),y.forEach(E,function(rt,Ze){(!(y.isUndefined(rt)||rt===null)&&r.call(n,rt,y.isString(Ze)?Ze.trim():Ze,L,M))===!0&&x(rt,L?L.concat(Ze):[Ze],W+1)}),d.pop()}}if(!y.isObject(t))throw new TypeError("data must be an object");return x(t),n}var Do=Hj;function UM(t){let n={"!":"%21","'":"%27","(":"%28",")":"%29","~":"%7E","%20":"+"};return encodeURIComponent(t).replace(/[!'()~]|%20/g,function(i){return n[i]})}function HM(t,n){this._pairs=[],t&&Do(t,this,n)}var zM=HM.prototype;zM.append=function(n,e){this._pairs.push([n,e])};zM.toString=function(n){let e=n?i=>n.call(this,i,UM):UM;return this._pairs.map(function(r){return e(r[0])+"="+e(r[1])},"").join("&")};var mm=HM;function zj(t){return encodeURIComponent(t).replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",").replace(/%20/g,"+")}function Id(t,n,e){if(!n)return t;t=t||"";let i=y.isFunction(e)?{serialize:e}:e,r=y.getSafeProp(i,"encode")||zj,o=y.getSafeProp(i,"serialize"),a;if(o?a=o(n,i):a=y.isURLSearchParams(n)?n.toString():new mm(n,i).toString(r),a){let s=t.indexOf("#");s!==-1&&(t=t.slice(0,s)),t+=(t.indexOf("?")===-1?"?":"&")+a}return t}var Vy=class{constructor(){this.handlers=[]}use(n,e,i){return this.handlers.push({fulfilled:n,rejected:e,synchronous:i?i.synchronous:!1,runWhen:i?i.runWhen:null}),this.handlers.length-1}eject(n){this.handlers[n]&&(this.handlers[n]=null)}clear(){this.handlers&&(this.handlers=[])}forEach(n){y.forEach(this.handlers,function(i){i!==null&&n(i)})}},jy=Vy;var ec={silentJSONParsing:!0,forcedJSONParsing:!0,clarifyTimeoutError:!1,legacyInterceptorReqResOrdering:!0,advertiseZstdAcceptEncoding:!1,validateStatusUndefinedResolves:!0};var $M=typeof URLSearchParams<"u"?URLSearchParams:mm;var GM=typeof FormData<"u"?FormData:null;var WM=typeof Blob<"u"?Blob:null;var qM={isBrowser:!0,classes:{URLSearchParams:$M,FormData:GM,Blob:WM},protocols:["http","https","file","blob","url","data"]};var Hy={};DT(Hy,{hasBrowserEnv:()=>Uy,hasStandardBrowserEnv:()=>$j,hasStandardBrowserWebWorkerEnv:()=>Gj,navigator:()=>By,origin:()=>Wj});var Uy=typeof window<"u"&&typeof document<"u",By=typeof navigator=="object"&&navigator||void 0,$j=Uy&&(!By||["ReactNative","NativeScript","NS"].indexOf(By.product)<0),Gj=typeof WorkerGlobalScope<"u"&&self instanceof WorkerGlobalScope&&typeof self.importScripts=="function",Wj=Uy&&window.location.href||"http://localhost";var lt=C(C({},Hy),qM);function zy(t,n){return Do(t,new lt.classes.URLSearchParams,C({visitor:function(e,i,r,o){return lt.isNode&&y.isBuffer(e)?(this.append(i,e.toString("base64")),!1):o.defaultVisitor.apply(this,arguments)}},n))}var YM=Ly;function KM(t){if(t>YM)throw new B("FormData field is too deeply nested ("+t+" levels). Max depth: "+YM,B.ERR_FORM_DATA_DEPTH_EXCEEDED)}function qj(t){let n=[],e=/[^.[\]]+|\[([^.[\]]*)]/g,i;for(;(i=e.exec(t))!==null;)KM(n.length),n.push(i[0]==="[]"?"":i[1]||i[0]);return n}function Yj(t){let n={},e=Object.keys(t),i,r=e.length,o;for(i=0;i<r;i++)o=e[i],n[o]=t[o];return n}function Kj(t){function n(e,i,r,o){KM(o);let a=e[o++];if(a==="__proto__")return!0;let s=Number.isFinite(+a),c=o>=e.length;return a=!a&&y.isArray(r)?r.length:a,c?(y.hasOwnProp(r,a)?r[a]=y.isArray(r[a])?r[a].concat(i):[r[a],i]:r[a]=i,!s):((!y.hasOwnProp(r,a)||!y.isObject(r[a]))&&(r[a]=[]),n(e,i,r[a],o)&&y.isArray(r[a])&&(r[a]=Yj(r[a])),!s)}if(y.isFormData(t)&&y.isFunction(t.entries)){let e={};return y.forEachEntry(t,(i,r)=>{n(qj(i),r,e,0)}),e}return null}var pm=Kj;var tc=(t,n)=>t!=null&&y.hasOwnProp(t,n)?t[n]:void 0;function Zj(t,n,e){if(y.isString(t))try{return(n||JSON.parse)(t),y.trim(t)}catch(i){if(i.name!=="SyntaxError")throw i}return(e||JSON.stringify)(t)}var $y={transitional:ec,adapter:["xhr","http","fetch"],transformRequest:[function(n,e){let i=e.getContentType()||"",r=i.indexOf("application/json")>-1,o=y.isObject(n);if(o&&y.isHTMLForm(n)&&(n=new FormData(n)),y.isFormData(n))return r?JSON.stringify(pm(n)):n;if(y.isArrayBuffer(n)||y.isBuffer(n)||y.isStream(n)||y.isFile(n)||y.isBlob(n)||y.isReadableStream(n))return n;if(y.isArrayBufferView(n))return n.buffer;if(y.isURLSearchParams(n))return e.setContentType("application/x-www-form-urlencoded;charset=utf-8",!1),n.toString();let s;if(o){let c=tc(this,"formSerializer");if(i.indexOf("application/x-www-form-urlencoded")>-1)return zy(n,c).toString();if((s=y.isFileList(n))||i.indexOf("multipart/form-data")>-1){let l=tc(this,"env"),d=l&&l.FormData;return Do(s?{"files[]":n}:n,d&&new d,c)}}return o||r?(e.setContentType("application/json",!1),Zj(n)):n}],transformResponse:[function(n){let e=tc(this,"transitional")||$y.transitional,i=e&&e.forcedJSONParsing,r=tc(this,"responseType"),o=r==="json";if(y.isResponse(n)||y.isReadableStream(n))return n;if(n&&y.isString(n)&&(i&&!r||o)){let s=!(e&&e.silentJSONParsing)&&o;try{return JSON.parse(n,tc(this,"parseReviver"))}catch(c){if(s)throw c.name==="SyntaxError"?B.from(c,B.ERR_BAD_RESPONSE,this,null,tc(this,"response")):c}}return n}],timeout:0,xsrfCookieName:"XSRF-TOKEN",xsrfHeaderName:"X-XSRF-TOKEN",maxContentLength:-1,maxBodyLength:-1,env:{FormData:lt.classes.FormData,Blob:lt.classes.Blob},validateStatus:function(n){return n>=200&&n<300},headers:{common:{Accept:"application/json, text/plain, */*","Content-Type":void 0}}};y.forEach(["delete","get","head","post","put","patch","query"],t=>{$y.headers[t]={}});var nc=$y;function Nd(t,n){let e=this||nc,i=n||e,r=gt.from(i.headers),o=i.data;return y.forEach(t,function(s){o=s.call(e,o,r.normalize(),n?n.status:void 0)}),r.normalize(),o}function Td(t){return!!(t&&t.__CANCEL__)}var Gy=class extends B{constructor(n,e,i){super(n??"canceled",B.ERR_CANCELED,e,i),this.name="CanceledError",this.__CANCEL__=!0}},ar=Gy;function kd(t,n,e){let i=e.config.validateStatus;!e.status||!i||i(e.status)?t(e):n(new B("Request failed with status code "+e.status,e.status>=400&&e.status<500?B.ERR_BAD_REQUEST:B.ERR_BAD_RESPONSE,e.config,e.request,e))}function Wy(t){let n=/^([-+\w]{1,25}):(?:\/\/)?/.exec(t);return n&&n[1]||""}function Qj(t,n){t=t||10;let e=new Array(t),i=new Array(t),r=0,o=0,a;return n=n!==void 0?n:1e3,function(c){let l=Date.now(),d=i[o];a||(a=l),e[r]=c,i[r]=l;let f=o,p=0;for(;f!==r;)p+=e[f++],f=f%t;if(r=(r+1)%t,r===o&&(o=(o+1)%t),l-a<n)return;let _=d&&l-d;return _?Math.round(p*1e3/_):void 0}}var ZM=Qj;function Xj(t,n){let e=0,i=1e3/n,r,o,a=(l,d=Date.now())=>{e=d,r=null,o&&(clearTimeout(o),o=null),t(...l)};return[(...l)=>{let d=Date.now(),f=d-e;f>=i?a(l,d):(r=l,o||(o=setTimeout(()=>{o=null,a(r)},i-f)))},()=>r&&a(r)]}var QM=Xj;var ic=(t,n,e=3)=>{let i=0,r=ZM(50,250);return QM(o=>{if(!o||typeof o.loaded!="number")return;let a=o.loaded,s=o.lengthComputable?o.total:void 0,c=Math.max(0,s!=null?Math.min(a,s):a),l=Math.max(0,c-i),d=r(l);i=Math.max(i,c);let f={loaded:c,total:s,progress:s?c/s:void 0,bytes:l,rate:d||void 0,estimated:d&&s?(s-c)/d:void 0,event:o,lengthComputable:s!=null,[n?"download":"upload"]:!0};t(f)},e)},qy=(t,n)=>{let e=t!=null;return[i=>n[0]({lengthComputable:e,total:t,loaded:i}),n[1]]},Yy=(t,n=y.asap)=>(...e)=>n(()=>t(...e));var XM=lt.hasStandardBrowserEnv?((t,n)=>e=>(e=new URL(e,lt.origin),t.protocol===e.protocol&&t.host===e.host&&(n||t.port===e.port)))(new URL(lt.origin),lt.navigator&&/(msie|trident)/i.test(lt.navigator.userAgent)):()=>!0;var JM=lt.hasStandardBrowserEnv?{write(t,n,e,i,r,o,a){if(typeof document>"u")return;let s=[`${t}=${encodeURIComponent(n)}`];y.isNumber(e)&&s.push(`expires=${new Date(e).toUTCString()}`),y.isString(i)&&s.push(`path=${i}`),y.isString(r)&&s.push(`domain=${r}`),o===!0&&s.push("secure"),y.isString(a)&&s.push(`SameSite=${a}`),document.cookie=s.join("; ")},read(t){if(typeof document>"u")return null;let n=document.cookie.split(";");for(let e=0;e<n.length;e++){let i=n[e].replace(/^\s+/,""),r=i.indexOf("=");if(r!==-1&&i.slice(0,r)===t)try{return decodeURIComponent(i.slice(r+1))}catch{return i.slice(r+1)}}return null},remove(t){this.write(t,"",Date.now()-864e5,"/")}}:{write(){},read(){return null},remove(){}};function Ky(t){return typeof t!="string"?!1:/^([a-z][a-z\d+\-.]*:)?\/\//i.test(t)}function Zy(t,n){if(!n)return t;let e=t.length;for(;e>0&&t.charCodeAt(e-1)===47;)e--;return t.slice(0,e)+"/"+n.replace(/^\/+/,"")}var Jj=/^https?:(?!\/\/)/i,eB=/[\t\n\r]/g;function tB(t){let n=0;for(;n<t.length&&t.charCodeAt(n)<=32;)n++;return t.slice(n)}function nB(t){return tB(t).replace(eB,"")}function iB(t){return t&&t.replace(/(^|&)([^=&]*=)?[^&]+/g,(n,e,i="")=>`${e}${i}${Md}`)}function rB(t){let n=t.replace(/^(https?:\/{0,2})[^/?#]*@/i,`$1${Md}@`),e=n.indexOf("#"),r=(e===-1?n:n.slice(0,e)).replace(/([?&][^=&#]*=)[^&#]*/g,`$1${Md}`);return e===-1?r:`${r}#${iB(n.slice(e+1))}`}function eI(t,n){if(typeof t=="string"){let e=nB(t);if(Jj.test(e))throw new B(`Invalid URL ${JSON.stringify(rB(e))}: missing "//" after protocol`,B.ERR_INVALID_URL,n)}}function Ad(t,n,e,i){eI(n,i);let r=!Ky(n);return t&&(r||e===!1)?(eI(t,i),Zy(t,n)):n}var tI=t=>t instanceof gt?C({},t):t,oB=t=>Object.getOwnPropertySymbols&&Object.getOwnPropertyDescriptor?Object.keys(t).concat(Object.getOwnPropertySymbols(t).filter(n=>Object.getOwnPropertyDescriptor(t,n).enumerable)):Object.keys(t);function ki(t,n){t=t||{},n=n||{};let e=Object.create(null);Object.defineProperty(e,"hasOwnProperty",{__proto__:null,value:Object.prototype.hasOwnProperty,enumerable:!1,writable:!0,configurable:!0});function i(d,f,p,_){return y.isPlainObject(d)&&y.isPlainObject(f)?y.merge.call({caseless:_},d,f):y.isPlainObject(f)?y.merge({},f):y.isArray(f)?f.slice():f}function r(d,f,p,_){if(y.isUndefined(f)){if(!y.isUndefined(d))return i(void 0,d,p,_)}else return i(d,f,p,_)}function o(d,f){if(!y.isUndefined(f))return i(void 0,f)}function a(d,f){if(y.isUndefined(f)){if(!y.isUndefined(d))return i(void 0,d)}else return i(void 0,f)}function s(d){let f=y.hasOwnProp(n,"transitional")?n.transitional:void 0;if(!y.isUndefined(f))if(y.isPlainObject(f)){if(y.hasOwnProp(f,d))return f[d]}else return;let p=y.hasOwnProp(t,"transitional")?t.transitional:void 0;if(y.isPlainObject(p)&&y.hasOwnProp(p,d))return p[d]}function c(d,f,p){if(y.hasOwnProp(n,p))return i(d,f);if(y.hasOwnProp(t,p))return i(void 0,d)}let l={url:o,method:o,data:o,baseURL:a,transformRequest:a,transformResponse:a,paramsSerializer:a,timeout:a,timeoutMessage:a,withCredentials:a,withXSRFToken:a,adapter:a,responseType:a,xsrfCookieName:a,xsrfHeaderName:a,onUploadProgress:a,onDownloadProgress:a,decompress:a,maxContentLength:a,maxBodyLength:a,beforeRedirect:a,transport:a,httpAgent:a,httpsAgent:a,cancelToken:a,socketPath:a,allowedSocketPaths:a,responseEncoding:a,validateStatus:c,headers:(d,f,p)=>r(tI(d),tI(f),p,!0)};return y.forEach(oB(C(C({},t),n)),function(f){if(f==="__proto__"||f==="constructor"||f==="prototype")return;let p=y.hasOwnProp(l,f)?l[f]:r,_=y.hasOwnProp(t,f)?t[f]:void 0,b=y.hasOwnProp(n,f)?n[f]:void 0,M=p(_,b,f);y.isUndefined(M)&&p!==c||(e[f]=M)}),y.hasOwnProp(n,"validateStatus")&&y.isUndefined(n.validateStatus)&&s("validateStatusUndefinedResolves")===!1&&(y.hasOwnProp(t,"validateStatus")?e.validateStatus=i(void 0,t.validateStatus):delete e.validateStatus),e}var aB=["content-type","content-length"];function Qy(t,n,e){if(e!=="content-only"){t.set(n);return}Object.entries(n||{}).forEach(([i,r])=>{aB.includes(i.toLowerCase())&&t.set(i,r)})}var sB=t=>encodeURIComponent(t).replace(/%([0-9A-F]{2})/gi,(n,e)=>String.fromCharCode(parseInt(e,16)));function cB(t){let n=ki({},t),e=p=>y.hasOwnProp(n,p)?n[p]:void 0,i=e("data"),r=e("withXSRFToken"),o=e("xsrfHeaderName"),a=e("xsrfCookieName"),s=e("headers"),c=e("auth"),l=e("baseURL"),d=e("allowAbsoluteUrls"),f=e("url");if(n.headers=s=gt.from(s),n.url=Id(Ad(l,f,d,n),e("params"),e("paramsSerializer")),c){let p=y.getSafeProp(c,"username")||"",_=y.getSafeProp(c,"password")||"";try{s.set("Authorization","Basic "+btoa(p+":"+(_?sB(_):"")))}catch(b){throw B.from(b,B.ERR_BAD_OPTION_VALUE,t)}}if(y.isFormData(i)&&(lt.hasStandardBrowserEnv||lt.hasStandardBrowserWebWorkerEnv||y.isReactNative(i)?s.setContentType(void 0):y.isFunction(i.getHeaders)&&Qy(s,i.getHeaders(),e("formDataHeaderPolicy"))),lt.hasStandardBrowserEnv&&(y.isFunction(r)&&(r=r(n)),r===!0||r==null&&XM(n.url))){let _=o&&a&&JM.read(a);_&&s.set(o,_)}return n}var gm=cB;var lB=typeof XMLHttpRequest<"u",nI=lB&&function(t){return new Promise(function(e,i){let r=gm(t),o=r.data,a=gt.from(r.headers).normalize(),{responseType:s,onUploadProgress:c,onDownloadProgress:l}=r,d,f,p,_,b;function M(){_&&_(),b&&b(),r.cancelToken&&r.cancelToken.unsubscribe(d),r.signal&&r.signal.removeEventListener("abort",d)}let x=new XMLHttpRequest;x.open(r.method.toUpperCase(),r.url,!0),x.timeout=r.timeout;function E(){if(!x)return;let W=gt.from("getAllResponseHeaders"in x&&x.getAllResponseHeaders()),rt={data:!s||s==="text"||s==="json"?x.responseText:x.response,status:x.status,statusText:x.statusText,headers:W,config:t,request:x};kd(function(_n){e(_n),M()},function(_n){i(_n),M()},rt),x=null}"onloadend"in x?x.onloadend=E:x.onreadystatechange=function(){!x||x.readyState!==4||x.status===0&&!(x.responseURL&&x.responseURL.startsWith("file:"))||setTimeout(E)},x.onabort=function(){x&&(i(new B("Request aborted",B.ECONNABORTED,t,x)),M(),x=null)},x.onerror=function(Me){let rt=Me&&Me.message?Me.message:"Network Error",Ze=new B(rt,B.ERR_NETWORK,t,x);Ze.event=Me||null,i(Ze),M(),x=null},x.ontimeout=function(){let Me=r.timeout?"timeout of "+r.timeout+"ms exceeded":"timeout exceeded",rt=r.transitional||ec;r.timeoutErrorMessage&&(Me=r.timeoutErrorMessage),i(new B(Me,rt.clarifyTimeoutError?B.ETIMEDOUT:B.ECONNABORTED,t,x)),M(),x=null},o===void 0&&a.setContentType(null),"setRequestHeader"in x&&y.forEach(fm(a),function(Me,rt){x.setRequestHeader(rt,Me)}),y.isUndefined(r.withCredentials)||(x.withCredentials=!!r.withCredentials),s&&s!=="json"&&(x.responseType=r.responseType),l&&([p,b]=ic(l,!0),x.addEventListener("progress",p)),c&&x.upload&&([f,_]=ic(c),x.upload.addEventListener("progress",f),x.upload.addEventListener("loadend",_)),(r.cancelToken||r.signal)&&(d=W=>{x&&(i(!W||W.type?new ar(null,t,x):W),x.abort(),M(),x=null)},r.cancelToken&&r.cancelToken.subscribe(d),r.signal&&(r.signal.aborted?d():r.signal.addEventListener("abort",d)));let L=Wy(r.url);if(L&&!lt.protocols.includes(L)){i(new B("Unsupported protocol "+L+":",B.ERR_BAD_REQUEST,t)),M();return}x.send(o||null)})};var dB=(t,n)=>{if(t=t?t.filter(Boolean):[],!n&&!t.length)return;let e=new AbortController,i=!1,r=function(c){if(!i){i=!0,a();let l=c instanceof Error?c:this.reason;e.abort(l instanceof B?l:new ar(l instanceof Error?l.message:l))}},o=n&&setTimeout(()=>{o=null,r(new B(`timeout of ${n}ms exceeded`,B.ETIMEDOUT))},n),a=()=>{t&&(o&&clearTimeout(o),o=null,t.forEach(c=>{c.unsubscribe?c.unsubscribe(r):c.removeEventListener("abort",r)}),t=null)};t.forEach(c=>{if(!i){if(c.aborted){r.call(c);return}c.addEventListener("abort",r,{once:!0})}});let{signal:s}=e;return s.unsubscribe=()=>y.asap(a),s},iI=dB;var uB=function*(t,n){let e=t.byteLength;if(!n||e<n){yield t;return}let i=0,r;for(;i<e;)r=i+n,yield t.slice(i,r),i=r},fB=async function*(t,n){for await(let e of hB(t))yield*uB(e,n)},hB=async function*(t){if(t[Symbol.asyncIterator]){yield*t;return}let n=t.getReader();try{for(;;){let{done:e,value:i}=await n.read();if(e)break;yield i}}finally{await n.cancel()}},Xy=(t,n,e,i)=>{let r=fB(t,n),o=0,a,s=c=>{a||(a=!0,i&&i(c))};return new ReadableStream({async pull(c){try{let{done:l,value:d}=await r.next();if(l){s(),c.close();return}let f=d.byteLength;if(e){let p=o+=f;e(p)}c.enqueue(new Uint8Array(d))}catch(l){throw s(l),l}},cancel(c){return s(c),r.return()}},{highWaterMark:2})};var rI=t=>t>=48&&t<=57||t>=65&&t<=70||t>=97&&t<=102,aI=(t,n,e)=>n+2<e&&rI(t.charCodeAt(n+1))&&rI(t.charCodeAt(n+2)),oI=t=>t<=57?t-48:(t&223)-55,mB=t=>t>=65&&t<=90||t>=97&&t<=122||t>=48&&t<=57||t===43||t===47||t===45||t===95,pB=t=>t===9||t===10||t===12||t===13||t===32,gB=t=>{let n=Math.floor(t/4),e=t%4;return n*3+(e===2?1:e===3?2:0)},_B=t=>{let n=t.length,e=0;return n>0&&t.charCodeAt(n-1)===61&&(e++,n>1&&t.charCodeAt(n-2)===61&&e++),Math.floor((n-e)*3/4)},vB=t=>{let n=t.length,e=0,i=0,r=!1;for(let o=0;o<n;o++){let a=t.charCodeAt(o);if(a===37&&aI(t,o,n)&&(a=oI(t.charCodeAt(o+1))*16+oI(t.charCodeAt(o+2)),o+=2),!pB(a)){if(a===61){i++;continue}if(!mB(a)||i>0){r=!0;continue}e++}}return r||i>2||i>0&&(e+i)%4!==0||e%4===1?_B(t):gB(e)},yB=(t,n)=>{if(!t||typeof t!="string"||!t.startsWith("data:"))return 0;let e=t.indexOf(",");if(e<0)return 0;let i=t.slice(5,e),r=t.slice(e+1);if(/;base64/i.test(i))return n(r);let a=0;for(let s=0,c=r.length;s<c;s++){let l=r.charCodeAt(s);if(l===37&&aI(r,s,c))a+=1,s+=2;else if(l<128)a+=1;else if(l<2048)a+=2;else if(l>=55296&&l<=56319&&s+1<c){let d=r.charCodeAt(s+1);d>=56320&&d<=57343?(a+=4,s++):a+=3}else a+=3}return a};function Jy(t){let n=typeof t=="string"?t.indexOf("#"):-1;return yB(n===-1?t:t.slice(0,n),vB)}var rc="1.19.0";var sI=64*1024,{isFunction:_m}=y,bB=t=>encodeURIComponent(t).replace(/%([0-9A-F]{2})/gi,(n,e)=>String.fromCharCode(parseInt(e,16))),cI=t=>{if(!y.isString(t))return t;try{return decodeURIComponent(t)}catch{return t}},lI=(t,...n)=>{try{return!!t(...n)}catch{return!1}},CB=t=>{let n=t.indexOf("://"),e=t;return n!==-1&&(e=e.slice(n+3)),e.includes("@")||e.includes(":")},wB=t=>{let n=y.global!==void 0&&y.global!==null?y.global:globalThis,{ReadableStream:e,TextEncoder:i}=n;t=y.merge.call({skipUndefined:!0},{Request:n.Request,Response:n.Response},t);let{fetch:r,Request:o,Response:a}=t,s=r?_m(r):typeof fetch=="function",c=_m(o),l=_m(a);if(!s)return!1;let d=s&&_m(e),f=s&&(typeof i=="function"?(E=>L=>E.encode(L))(new i):async E=>new Uint8Array(await new o(E).arrayBuffer())),p=c&&d&&lI(()=>{let E=!1,L=new o(lt.origin,{body:new e,method:"POST",get duplex(){return E=!0,"half"}}),W=L.headers.has("Content-Type");return L.body!=null&&L.body.cancel(),E&&!W}),_=l&&d&&lI(()=>y.isReadableStream(new a("").body)),b={stream:_&&(E=>E.body)};s&&["text","arrayBuffer","blob","formData","stream"].forEach(E=>{!b[E]&&(b[E]=(L,W)=>{let Me=L&&L[E];if(Me)return Me.call(L);throw new B(`Response type '${E}' is not supported`,B.ERR_NOT_SUPPORT,W)})});let M=async E=>{if(E==null)return 0;if(y.isBlob(E))return E.size;if(y.isSpecCompliantForm(E))return(await new o(lt.origin,{method:"POST",body:E}).arrayBuffer()).byteLength;if(y.isArrayBufferView(E)||y.isArrayBuffer(E))return E.byteLength;if(y.isURLSearchParams(E)&&(E=E+""),y.isString(E))return(await f(E)).byteLength},x=async(E,L)=>{let W=y.toFiniteNumber(E.getContentLength());return W??M(L)};return async E=>{let{url:L,method:W,data:Me,signal:rt,cancelToken:Ze,timeout:_n,onDownloadProgress:Vi,onUploadProgress:Yn,responseType:ji,headers:Kn,withCredentials:$r="same-origin",fetchOptions:xc,maxContentLength:Bi,maxBodyLength:Jd}=gm(E),Mc=y.isNumber(Bi)&&Bi>-1,lp=y.isNumber(Jd)&&Jd>-1,_T=Qe=>y.hasOwnProp(E,Qe)?E[Qe]:void 0,o0=r||fetch;ji=ji?(ji+"").toLowerCase():"text";let Gr=iI([rt,Ze&&Ze.toAbortSignal()],_n),zt=null,Ro=Gr&&Gr.unsubscribe&&(()=>{Gr.unsubscribe()}),Ya,Ic=null,a0=()=>new B("Request body larger than maxBodyLength limit",B.ERR_BAD_REQUEST,E,zt);try{let Qe,Zn=_T("auth");if(Zn){let Ce=y.getSafeProp(Zn,"username")||"",An=y.getSafeProp(Zn,"password")||"";Qe={username:Ce,password:An}}if(CB(L)){let Ce=new URL(L,lt.origin);if(!Qe&&(Ce.username||Ce.password)){let An=cI(Ce.username),Wr=cI(Ce.password);Qe={username:An,password:Wr}}(Ce.username||Ce.password)&&(Ce.username="",Ce.password="",L=Ce.href)}if(Qe&&(Kn.delete("authorization"),Kn.set("Authorization","Basic "+btoa(bB((Qe.username||"")+":"+(Qe.password||""))))),Mc&&typeof L=="string"&&L.startsWith("data:")&&Jy(L)>Bi)throw new B("maxContentLength size of "+Bi+" exceeded",B.ERR_BAD_RESPONSE,E,zt);if(lp&&W!=="get"&&W!=="head"){let Ce=await M(Me);if(typeof Ce=="number"&&isFinite(Ce)&&(Ya=Ce,Ce>Jd))throw a0()}let eu=lp&&(y.isReadableStream(Me)||y.isStream(Me)),s0=(Ce,An,Wr)=>Xy(Ce,sI,Oo=>{if(lp&&Oo>Jd)throw Ic=a0();An&&An(Oo)},Wr);if(p&&W!=="get"&&W!=="head"&&(Yn||eu)){if(Ya=Ya??await x(Kn,Me),Ya!==0||eu){let Ce=new o(L,{method:"POST",body:Me,duplex:"half"}),An;if(y.isFormData(Me)&&(An=Ce.headers.get("content-type"))&&Kn.setContentType(An),Ce.body){let[Wr,Oo]=Yn&&qy(Ya,ic(Yy(Yn)))||[];Me=s0(Ce.body,Wr,Oo)}}}else if(eu&&!c&&d&&W!=="get"&&W!=="head")Me=s0(Me);else if(eu&&c&&!p&&W!=="get"&&W!=="head")throw new B("Stream request bodies are not supported by the current fetch implementation",B.ERR_NOT_SUPPORT,E,zt);y.isString($r)||($r=$r?"include":"omit");let vT=c&&"credentials"in o.prototype;if(y.isFormData(Me)){let Ce=Kn.getContentType();Ce&&/^multipart\/form-data/i.test(Ce)&&!/boundary=/i.test(Ce)&&Kn.delete("content-type")}Kn.set("User-Agent","axios/"+rc,!1);let c0=re(C({},xc),{signal:Gr,method:W.toUpperCase(),headers:fm(Kn.normalize()),body:Me,duplex:"half",credentials:vT?$r:void 0});zt=c&&new o(L,c0);let ur=await(c?o0(zt,xc):o0(L,c0)),l0=gt.from(ur.headers);if(Mc){let Ce=y.toFiniteNumber(l0.getContentLength());if(Ce!=null&&Ce>Bi)throw new B("maxContentLength size of "+Bi+" exceeded",B.ERR_BAD_RESPONSE,E,zt)}let dp=_&&(ji==="stream"||ji==="response");if(_&&ur.body&&(Vi||Mc||dp&&Ro)){let Ce={};["status","statusText","headers"].forEach(Nc=>{Ce[Nc]=ur[Nc]});let An=y.toFiniteNumber(l0.getContentLength()),[Wr,Oo]=Vi&&qy(An,ic(Yy(Vi),!0))||[],d0=0,yT=Nc=>{if(Mc&&(d0=Nc,d0>Bi))throw new B("maxContentLength size of "+Bi+" exceeded",B.ERR_BAD_RESPONSE,E,zt);Wr&&Wr(Nc)};ur=new a(Xy(ur.body,sI,yT,()=>{Oo&&Oo(),Ro&&Ro()}),Ce)}ji=ji||"text";let fr=await b[y.findKey(b,ji)||"text"](ur,E);if(Mc&&!_&&!dp){let Ce;if(fr!=null&&(typeof fr.byteLength=="number"?Ce=fr.byteLength:typeof fr.size=="number"?Ce=fr.size:typeof fr=="string"&&(Ce=typeof i=="function"?new i().encode(fr).byteLength:fr.length)),typeof Ce=="number"&&Ce>Bi)throw new B("maxContentLength size of "+Bi+" exceeded",B.ERR_BAD_RESPONSE,E,zt)}return!dp&&Ro&&Ro(),await new Promise((Ce,An)=>{kd(Ce,An,{data:fr,headers:gt.from(ur.headers),status:ur.status,statusText:ur.statusText,config:E,request:zt})})}catch(Qe){if(Ro&&Ro(),Gr&&Gr.aborted&&Gr.reason instanceof B){let Zn=Gr.reason;throw Zn.config=E,zt&&(Zn.request=zt),Qe!==Zn&&Object.defineProperty(Zn,"cause",{__proto__:null,value:Qe,writable:!0,enumerable:!1,configurable:!0}),Zn}if(Ic)throw zt&&!Ic.request&&(Ic.request=zt),Ic;if(Qe instanceof B)throw zt&&!Qe.request&&(Qe.request=zt),Qe;if(Qe&&Qe.name==="TypeError"&&/Load failed|fetch/i.test(Qe.message)){let Zn=new B("Network Error",B.ERR_NETWORK,E,zt,Qe&&Qe.response);throw Object.defineProperty(Zn,"cause",{__proto__:null,value:Qe.cause||Qe,writable:!0,enumerable:!1,configurable:!0}),Zn}throw B.from(Qe,Qe&&Qe.code,E,zt,Qe&&Qe.response)}}},SB=new Map,eb=t=>{let n=t&&t.env||{},{fetch:e,Request:i,Response:r}=n,o=[i,r,e],a=o.length,s=a,c,l,d=SB;for(;s--;)c=o[s],l=d.get(c),l===void 0&&d.set(c,l=s?new Map:wB(n)),d=l;return l},$se=eb();var tb={http:So,xhr:nI,fetch:{get:eb}};y.forEach(tb,(t,n)=>{if(t){try{Object.defineProperty(t,"name",{__proto__:null,value:n})}catch{}Object.defineProperty(t,"adapterName",{__proto__:null,value:n})}});var dI=t=>`- ${t}`,EB=t=>y.isFunction(t)||t===null||t===!1;function xB(t,n){t=y.isArray(t)?t:[t];let{length:e}=t,i,r,o={};for(let a=0;a<e;a++){i=t[a];let s;if(r=i,!EB(i)&&(r=tb[(s=String(i)).toLowerCase()],r===void 0))throw new B(`Unknown adapter '${s}'`);if(r&&(y.isFunction(r)||(r=r.get(n))))break;o[s||"#"+a]=r}if(!r){let a=Object.entries(o).map(([c,l])=>`adapter ${c} `+(l===!1?"is not supported by the environment":"is not available in the build")),s=e?a.length>1?`since :
`+a.map(dI).join(`
`):" "+dI(a[0]):"as no adapter specified";throw new B("There is no suitable adapter to dispatch the request "+s,B.ERR_NOT_SUPPORT)}return r}var vm={getAdapter:xB,adapters:tb};function nb(t){if(t.cancelToken&&t.cancelToken.throwIfRequested(),t.signal&&t.signal.aborted)throw new ar(null,t)}function Rd(t){return nb(t),t.headers=gt.from(t.headers),t.data=Nd.call(t,t.transformRequest),["post","put","patch"].indexOf(t.method)!==-1&&t.headers.setContentType("application/x-www-form-urlencoded",!1),vm.getAdapter(t.adapter||nc.adapter,t)(t).then(function(i){nb(t),t.response=i;try{i.data=Nd.call(t,t.transformResponse,i)}finally{delete t.response}return i.headers=gt.from(i.headers),i},function(i){if(!Td(i)&&(nb(t),i&&i.response)){t.response=i.response;try{i.response.data=Nd.call(t,t.transformResponse,i.response)}finally{delete t.response}i.response.headers=gt.from(i.response.headers)}return Promise.reject(i)})}var ym={};["object","boolean","number","function","string","symbol"].forEach((t,n)=>{ym[t]=function(i){return typeof i===t||"a"+(n<1?"n ":" ")+t}});var uI={};ym.transitional=function(n,e,i){function r(o,a){return"[Axios v"+rc+"] Transitional option '"+o+"'"+a+(i?". "+i:"")}return(o,a,s)=>{if(n===!1)throw new B(r(a," has been removed"+(e?" in "+e:"")),B.ERR_DEPRECATED);return e&&!uI[a]&&(uI[a]=!0,console.warn(r(a," has been deprecated since v"+e+" and will be removed in the near future"))),n?n(o,a,s):!0}};ym.spelling=function(n){return(e,i)=>(console.warn(`${i} is likely a misspelling of ${n}`),!0)};function MB(t,n,e){if(typeof t!="object"||t===null)throw new B("options must be an object",B.ERR_BAD_OPTION_VALUE);let i=Object.keys(t),r=i.length;for(;r-- >0;){let o=i[r],a=Object.prototype.hasOwnProperty.call(n,o)?n[o]:void 0;if(a){let s=t[o],c=s===void 0||a(s,o,t);if(c!==!0)throw new B("option "+o+" must be "+c,B.ERR_BAD_OPTION_VALUE);continue}if(e!==!0)throw new B("Unknown option "+o,B.ERR_BAD_OPTION)}}var Od={assertOptions:MB,validators:ym};var rn=Od.validators,oc=class{constructor(n){this.defaults=n||{},this.interceptors={request:new jy,response:new jy}}async request(n,e){try{return await this._request(n,e)}catch(i){if(i instanceof Error){let r={};Error.captureStackTrace?Error.captureStackTrace(r):r=new Error;let o=(()=>{if(!r.stack)return"";let a=r.stack.indexOf(`
`);return a===-1?"":r.stack.slice(a+1)})();try{if(!i.stack)i.stack=o;else if(o){let a=o.indexOf(`
`),s=a===-1?-1:o.indexOf(`
`,a+1),c=s===-1?"":o.slice(s+1);String(i.stack).endsWith(c)||(i.stack+=`
`+o)}}catch{}}throw i}}_request(n,e){typeof n=="string"?(e=e||{},e.url=n):e=n||{},e=ki(this.defaults,e);let{transitional:i,paramsSerializer:r,headers:o}=e;i!==void 0&&Od.assertOptions(i,{silentJSONParsing:rn.transitional(rn.boolean),forcedJSONParsing:rn.transitional(rn.boolean),clarifyTimeoutError:rn.transitional(rn.boolean),legacyInterceptorReqResOrdering:rn.transitional(rn.boolean),advertiseZstdAcceptEncoding:rn.transitional(rn.boolean),validateStatusUndefinedResolves:rn.transitional(rn.boolean)},!1),r!=null&&(y.isFunction(r)?e.paramsSerializer={serialize:r}:Od.assertOptions(r,{encode:rn.function,serialize:rn.function},!0)),e.allowAbsoluteUrls!==void 0||(this.defaults.allowAbsoluteUrls!==void 0?e.allowAbsoluteUrls=this.defaults.allowAbsoluteUrls:e.allowAbsoluteUrls=!0),Od.assertOptions(e,{baseUrl:rn.spelling("baseURL"),withXsrfToken:rn.spelling("withXSRFToken")},!0),e.method=(e.method||this.defaults.method||"get").toLowerCase();let a=o&&y.merge(o.common,o[e.method]);o&&y.forEach(["delete","get","head","post","put","patch","query","common"],b=>{delete o[b]}),e.headers=gt.concat(a,o);let s=[],c=!0;this.interceptors.request.forEach(function(M){if(typeof M.runWhen=="function"&&M.runWhen(e)===!1)return;c=c&&M.synchronous;let x=e.transitional||ec;x&&x.legacyInterceptorReqResOrdering?s.unshift(M.fulfilled,M.rejected):s.push(M.fulfilled,M.rejected)});let l=[];this.interceptors.response.forEach(function(M){l.push(M.fulfilled,M.rejected)});let d,f=0,p;if(!c){let b=[Rd.bind(this),void 0];for(b.unshift(...s),b.push(...l),p=b.length,d=Promise.resolve(e);f<p;)d=d.then(b[f++],b[f++]);return d}p=s.length;let _=e;for(;f<p;){let b=s[f++],M=s[f++];try{_=b?b(_):_}catch(x){if(!M){d=Promise.reject(x);break}try{let E=M.call(this,x);y.isThenable(E)&&(d=Promise.resolve(E).then(()=>Rd.call(this,_)))}catch(E){d=Promise.reject(E)}break}}if(!d)try{d=Rd.call(this,_)}catch(b){d=Promise.reject(b)}for(f=0,p=l.length;f<p;)d=d.then(l[f++],l[f++]);return d}getUri(n){n=ki(this.defaults,n);let e=Ad(n.baseURL,n.url,n.allowAbsoluteUrls,n);return Id(e,n.params,n.paramsSerializer)}};y.forEach(["delete","get","head","options"],function(n){oc.prototype[n]=function(e,i){return this.request(ki(i||{},{method:n,url:e,data:i&&y.hasOwnProp(i,"data")?i.data:void 0}))}});y.forEach(["post","put","patch","query"],function(n){function e(i){return function(o,a,s){return this.request(ki(s||{},{method:n,headers:i?{"Content-Type":"multipart/form-data"}:{},url:o,data:a}))}}oc.prototype[n]=e(),n!=="query"&&(oc.prototype[n+"Form"]=e(!0))});var Pd=oc;var ib=class t{constructor(n){if(typeof n!="function")throw new TypeError("executor must be a function.");let e;this.promise=new Promise(function(o){e=o});let i=this;this.promise.then(r=>{if(!i._listeners)return;let o=i._listeners.length;for(;o-- >0;)i._listeners[o](r);i._listeners=null}),this.promise.then=r=>{let o,a=new Promise(s=>{i.subscribe(s),o=s}).then(r);return a.cancel=function(){i.unsubscribe(o)},a},n(function(o,a,s){i.reason||(i.reason=new ar(o,a,s),e(i.reason))})}throwIfRequested(){if(this.reason)throw this.reason}subscribe(n){if(this.reason){n(this.reason);return}this._listeners?this._listeners.push(n):this._listeners=[n]}unsubscribe(n){if(!this._listeners)return;let e=this._listeners.indexOf(n);e!==-1&&this._listeners.splice(e,1)}toAbortSignal(){let n=new AbortController,e=i=>{n.abort(i)};return this.subscribe(e),n.signal.unsubscribe=()=>this.unsubscribe(e),n.signal}static source(){let n;return{token:new t(function(r){n=r}),cancel:n}}},fI=ib;function rb(t){return function(e){return t.apply(null,e)}}function ob(t){return y.isObject(t)&&t.isAxiosError===!0}var ab={Continue:100,SwitchingProtocols:101,Processing:102,EarlyHints:103,Ok:200,Created:201,Accepted:202,NonAuthoritativeInformation:203,NoContent:204,ResetContent:205,PartialContent:206,MultiStatus:207,AlreadyReported:208,ImUsed:226,MultipleChoices:300,MovedPermanently:301,Found:302,SeeOther:303,NotModified:304,UseProxy:305,Unused:306,TemporaryRedirect:307,PermanentRedirect:308,BadRequest:400,Unauthorized:401,PaymentRequired:402,Forbidden:403,NotFound:404,MethodNotAllowed:405,NotAcceptable:406,ProxyAuthenticationRequired:407,RequestTimeout:408,Conflict:409,Gone:410,LengthRequired:411,PreconditionFailed:412,PayloadTooLarge:413,UriTooLong:414,UnsupportedMediaType:415,RangeNotSatisfiable:416,ExpectationFailed:417,ImATeapot:418,MisdirectedRequest:421,UnprocessableEntity:422,Locked:423,FailedDependency:424,TooEarly:425,UpgradeRequired:426,PreconditionRequired:428,TooManyRequests:429,RequestHeaderFieldsTooLarge:431,UnavailableForLegalReasons:451,InternalServerError:500,NotImplemented:501,BadGateway:502,ServiceUnavailable:503,GatewayTimeout:504,HttpVersionNotSupported:505,VariantAlsoNegotiates:506,InsufficientStorage:507,LoopDetected:508,NotExtended:510,NetworkAuthenticationRequired:511,WebServerReturnsAnUnknownError:520,WebServerIsDown:521,ConnectionTimedOut:522,OriginIsUnreachable:523,TimeoutOccurred:524,SslHandshakeFailed:525,InvalidSslCertificate:526};Object.entries(ab).forEach(([t,n])=>{ab[n]=t});var hI=ab;function mI(t){let n=new Pd(t),e=wd(Pd.prototype.request,n);return y.extend(e,Pd.prototype,n,{allOwnKeys:!0}),y.extend(e,n,null,{allOwnKeys:!0}),e.create=function(r){return mI(ki(t,r))},e}var kt=mI(nc);kt.Axios=Pd;kt.CanceledError=ar;kt.CancelToken=fI;kt.isCancel=Td;kt.VERSION=rc;kt.toFormData=Do;kt.AxiosError=B;kt.Cancel=kt.CanceledError;kt.all=function(n){return Promise.all(n)};kt.spread=rb;kt.isAxiosError=ob;kt.mergeConfig=ki;kt.AxiosHeaders=gt;kt.formToJSON=t=>pm(y.isHTMLForm(t)?new FormData(t):t);kt.getAdapter=vm.getAdapter;kt.HttpStatusCode=hI;kt.default=kt;var bm=kt;var{Axios:Hce,AxiosError:zce,CanceledError:$ce,isCancel:Gce,CancelToken:Wce,VERSION:qce,all:Yce,Cancel:Kce,isAxiosError:Zce,spread:Qce,toFormData:Xce,AxiosHeaders:Jce,HttpStatusCode:ele,formToJSON:tle,getAdapter:nle,mergeConfig:ile,create:rle}=bm;var ac=bm.create({baseURL:"https://toy.pequla.com/api",validateStatus:t=>t===200,headers:{Accept:"application/json"}}),pn=class{static async getToys(){return await ac.get("/toy")}static async getToyById(n){return await ac.get(`/toy/${n}`)}static async getToyByPermalink(n){return await ac.get(`/toy/permalink/${n}`)}static async getToysByIds(n){return await ac.request({url:"/toy/list",method:"post",data:n})}static async getAgeGroups(){return await ac.get("/age-group")}static async getTypes(){return await ac.get("/type")}};var IB=t=>["/igracke",t],NB=(t,n)=>n.typeId,TB=(t,n)=>n.toyId;function kB(t,n){if(t&1&&(m(0,"a",11)(1,"div",12)(2,"mat-icon"),v(3),h()(),m(4,"span"),v(5),h()()),t&2){let e=n.$implicit;g(3),G(e.icon),g(2),G(e.name)}}function AB(t,n){if(t&1&&(m(0,"div",9),at(1,kB,6,2,"a",11,NB),h()),t&2){let e=D();g(),st(e.categories())}}function RB(t,n){if(t&1&&(m(0,"mat-card",14),de(1,"img",15),m(2,"mat-card-content")(3,"h3"),v(4),h(),m(5,"p",16),v(6),h(),m(7,"p",17),v(8),h()(),m(9,"mat-card-actions")(10,"a",18),v(11,"Detalji"),h()()()),t&2){let e=n.$implicit,i=D(2);g(),T("src",i.imageBaseUrl+e.imageUrl,tr)("alt",e.name),g(3),G(e.name),g(2),pe("Uzrast: ",e.ageGroup.name),g(2),pe("",e.price," RSD"),g(2),T("routerLink",Ir(6,IB,e.permalink))}}function OB(t,n){if(t&1&&(m(0,"section",10)(1,"h2"),v(2,"\u2B50 Izdvojene igra\u010Dke"),h(),m(3,"div",13),at(4,RB,12,8,"mat-card",14,TB),h()()),t&2){let e=D();g(4),st(e.featuredToys())}}var PB={1:"extension",2:"menu_book",3:"accessibility_new",4:"palette",5:"directions_car",6:"pets",7:"casino",8:"construction",9:"music_note",10:"school"},Cm=class t{categories=S([]);featuredToys=S([]);imageBaseUrl="https://toy.pequla.com";ngOnInit(){this.loadCategories(),this.loadFeatured()}async loadCategories(){try{let n=await pn.getTypes();this.categories.set(n.data.map(e=>({typeId:e.typeId,name:e.name,icon:PB[e.typeId]??"toys"})))}catch{this.categories.set([])}}async loadFeatured(){try{let n=await pn.getToys();this.featuredToys.set(n.data.slice(0,6))}catch{this.featuredToys.set([])}}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=F({type:t,selectors:[["app-home"]],decls:19,vars:2,consts:[[1,"hero"],[1,"hero-columns"],[1,"hero-text"],[1,"brand-mark"],[1,"hero-actions"],["mat-raised-button","","routerLink","/igracke",1,"cta-button"],[1,"hero-illustration"],["src","hero-illustration.png","alt","Ilustracija kutije sa igra\u010Dkama",1,"hero-illustration-img"],["id","kategorije",1,"categories"],[1,"category-grid"],[1,"featured"],["routerLink","/igracke",1,"category-item"],[1,"category-circle"],[1,"featured-grid"],[1,"featured-card"],["mat-card-image","",3,"src","alt"],[1,"age-range"],[1,"price"],["mat-button","",3,"routerLink"]],template:function(e,i){e&1&&(m(0,"section",0)(1,"div",1)(2,"div",2)(3,"span",3),v(4,"Toy Box"),h(),m(5,"h1"),v(6,"Igra po\u010Dinje ovde!"),h(),m(7,"p"),v(8,"Prona\u0111i savr\u0161enu igra\u010Dku za svaki mali osmeh."),h(),m(9,"div",4)(10,"a",5),v(11,"Pogledaj igra\u010Dke \u2192"),h()()(),m(12,"div",6),de(13,"img",7),h()()(),m(14,"section",8)(15,"h2"),v(16,"Istra\u017Ei po kategoriji"),h(),R(17,AB,3,0,"div",9),h(),R(18,OB,6,0,"section",10)),e&2&&(g(17),O(i.categories().length>0?17:-1),g(),O(i.featuredToys().length>0?18:-1))},dependencies:[Ht,Vt,St,xn,En,vM,am,sm,In,Mn],styles:['[_nghost-%COMP%]{justify-content:flex-start;gap:16px;padding:16px 0 0;box-sizing:border-box}.hero[_ngcontent-%COMP%]{position:relative;overflow:hidden;max-width:1280px;margin:0 auto;padding:18px 20px;background:var(--%NS%mat-sys-tertiary-container);color:var(--%NS%mat-sys-on-tertiary-container);border-radius:16px}.hero[_ngcontent-%COMP%]:before, .hero[_ngcontent-%COMP%]:after{content:"";position:absolute;border-radius:50%;opacity:.35}.hero[_ngcontent-%COMP%]:before{width:120px;height:120px;background:#e66b60;top:-40px;right:60px}.hero-columns[_ngcontent-%COMP%]{position:relative;z-index:1;display:flex;align-items:center;justify-content:space-between;gap:32px;flex-wrap:wrap}.hero-text[_ngcontent-%COMP%]{flex:1 1 320px;display:flex;flex-direction:column;align-items:flex-start;text-align:left;gap:12px}.brand-mark[_ngcontent-%COMP%]{font-size:26px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--%NS%mat-sys-primary);margin-bottom:4px}.hero-text[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:36px;margin:0;line-height:1.15}.hero-text[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{font-size:16px;margin:0;max-width:420px}.hero-actions[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;margin-top:4px}.hero-illustration[_ngcontent-%COMP%]{flex:0 0 auto;display:flex;justify-content:center}.hero-illustration-img[_ngcontent-%COMP%]{width:100%;max-width:450px;height:auto;display:block}.logo[_ngcontent-%COMP%]{width:180px;height:auto}.cta-button[_ngcontent-%COMP%]{background:var(--%NS%mat-sys-primary)!important;color:var(--%NS%mat-sys-on-primary)!important;box-shadow:0 4px 12px #00000026}.categories[_ngcontent-%COMP%]{max-width:1280px;margin:0 auto;padding:0 24px;text-align:center}.categories[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{margin-bottom:20px}.category-grid[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;justify-content:center;gap:20px}.category-item[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:8px;text-decoration:none;color:inherit;width:90px}.category-circle[_ngcontent-%COMP%]{width:64px;height:64px;border-radius:50%;background:#e66b60;color:#fff;display:flex;align-items:center;justify-content:center;transition:transform .15s ease}.category-item[_ngcontent-%COMP%]:hover   .category-circle[_ngcontent-%COMP%]{transform:scale(1.08)}.category-item[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{font-size:13px;font-weight:600;text-align:center}.featured[_ngcontent-%COMP%]{max-width:1280px;margin:0 auto;padding:0 24px 24px;text-align:center}.featured[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{margin-bottom:20px}.featured-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;text-align:left}.featured-card[_ngcontent-%COMP%]   img[_ngcontent-%COMP%]{height:110px;object-fit:cover}.featured-card[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0 0 4px;font-size:14px}.age-range[_ngcontent-%COMP%]{margin:0 0 4px;font-size:12px;color:var(--%NS%mat-sys-outline)}.price[_ngcontent-%COMP%]{margin:0;font-weight:600;font-size:13px;color:var(--%NS%mat-sys-secondary)}']})};var SI=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ie(Te),ie(U))};static \u0275dir=P({type:t})}return t})(),DI=(()=>{class t extends SI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=en(t)))(r||t)}})();static \u0275dir=P({type:t,features:[Be]})}return t})(),xo=new w("");var FB={provide:xo,useExisting:Xt(()=>Tn),multi:!0};function LB(){let t=oi()?oi().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var VB=new w(""),Tn=(()=>{class t extends SI{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!LB())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ie(Te),ie(U),ie(VB,8))};static \u0275dir=P({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&I("input",function(a){return r._handleInput(a.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(a){return r._compositionEnd(a.target.value)})},standalone:!1,features:[Re([FB]),Be]})}return t})();function lb(t){return t==null||db(t)===0}function db(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Lr=new w(""),ub=new w(""),jB=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Ai=class{static min(n){return BB(n)}static max(n){return UB(n)}static required(n){return EI(n)}static requiredTrue(n){return HB(n)}static email(n){return zB(n)}static minLength(n){return $B(n)}static maxLength(n){return GB(n)}static pattern(n){return WB(n)}static nullValidator(n){return Sm()}static compose(n){return kI(n)}static composeAsync(n){return AI(n)}};function BB(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function UB(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function EI(t){return lb(t.value)?{required:!0}:null}function HB(t){return t.value===!0?null:{required:!0}}function zB(t){return lb(t.value)||jB.test(t.value)?null:{email:!0}}function $B(t){return n=>{let e=n.value?.length??db(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function GB(t){return n=>{let e=n.value?.length??db(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function WB(t){if(!t)return Sm;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(lb(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function Sm(t){return null}function xI(t){return t!=null}function MI(t){return Mr(t)?ot(t):t}function II(t){let n={};return t.forEach(e=>{n=e!=null?C(C({},n),e):n}),Object.keys(n).length===0?null:n}function NI(t,n){return n.map(e=>e(t))}function qB(t){return!t.validate}function TI(t){return t.map(n=>qB(n)?n:e=>n.validate(e))}function kI(t){if(!t)return null;let n=t.filter(xI);return n.length==0?null:function(e){return II(NI(e,n))}}function fb(t){return t!=null?kI(TI(t)):null}function AI(t){if(!t)return null;let n=t.filter(xI);return n.length==0?null:function(e){let i=NI(e,n).map(MI);return Uc(i).pipe(ge(II))}}function hb(t){return t!=null?AI(TI(t)):null}function pI(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function RI(t){return t._rawValidators}function OI(t){return t._rawAsyncValidators}function sb(t){return t?Array.isArray(t)?t:[t]:[]}function Dm(t,n){return Array.isArray(t)?t.includes(n):t===n}function gI(t,n){let e=sb(n);return sb(t).forEach(r=>{Dm(e,r)||e.push(r)}),e}function _I(t,n){return sb(n).filter(e=>!Dm(t,e))}var Em=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=fb(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=hb(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},sr=class extends Em{name;get formDirective(){return null}get path(){return null}};var Fd="VALID",wm="INVALID",sc="PENDING",Ld="DISABLED",Eo=class{},xm=class extends Eo{value;source;constructor(n,e){super(),this.value=n,this.source=e}},jd=class extends Eo{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},Bd=class extends Eo{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},cc=class extends Eo{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Mm=class extends Eo{source;constructor(n){super(),this.source=n}},lc=class extends Eo{source;constructor(n){super(),this.source=n}};function PI(t){return(Rm(t)?t.validators:t)||null}function YB(t){return Array.isArray(t)?fb(t):t||null}function FI(t,n){return(Rm(n)?n.asyncValidators:t)||null}function KB(t){return Array.isArray(t)?hb(t):t||null}function Rm(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function ZB(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new k(1e3,"");if(!LI(i,e))throw new k(1001,"")}function QB(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new k(-1002,"")})}var Im=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=S(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Se(this.statusReactive)}set status(n){Se(()=>this.statusReactive.set(n))}_status=ct(()=>this.statusReactive());statusReactive=S(void 0);get valid(){return this.status===Fd}get invalid(){return this.status===wm}get pending(){return this.status===sc}get disabled(){return this.status===Ld}get enabled(){return this.status!==Ld}errors;get pristine(){return Se(this.pristineReactive)}set pristine(n){Se(()=>this.pristineReactive.set(n))}_pristine=ct(()=>this.pristineReactive());pristineReactive=S(!0);get dirty(){return!this.pristine}get touched(){return Se(this.touchedReactive)}set touched(n){Se(()=>this.touchedReactive.set(n))}_touched=ct(()=>this.touchedReactive());touchedReactive=S(!1);get untouched(){return!this.touched}_events=new N;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(gI(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(gI(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(_I(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(_I(n,this._rawAsyncValidators))}hasValidator(n){return Dm(this._rawValidators,n)}hasAsyncValidator(n){return Dm(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(re(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Bd(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new Bd(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(re(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new jd(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new jd(!0,i))}markAsPending(n={}){this.status=sc;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new cc(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(re(C({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Ld,this.errors=null,this._forEachChild(r=>{r.disable(re(C({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new xm(this.value,i)),this._events.next(new cc(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(re(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Fd,this._forEachChild(i=>{i.enable(re(C({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(re(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Fd||this.status===sc)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new xm(this.value,e)),this._events.next(new cc(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(re(C({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Ld:Fd}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=sc,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=MI(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new cc(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new A,this.statusChanges=new A}_calculateStatus(){return this._allControlsDisabled()?Ld:this.errors?wm:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(sc)?sc:this._anyControlsHaveStatus(wm)?wm:Fd}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new jd(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new Bd(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Rm(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=YB(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=KB(this._rawAsyncValidators)}_updateHasRequiredValidator(){Se(()=>this._hasRequired.set(this.hasValidator(Ai.required)))}};function LI(t,n){return Object.hasOwn(t,n)}function XB(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function JB(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var cb=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var eU=(()=>{class t{_validator=Sm;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):Sm,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,features:[Pe]})}return t})();var tU={provide:Lr,useExisting:Xt(()=>dc),multi:!0};var dc=(()=>{class t extends eU{required;inputName="required";normalizeInput=j;createValidator=e=>EI;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=en(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&ee("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Re([tU]),Be]})}return t})();var nU=new w(""),Om=new w("",{factory:()=>mb}),mb="always";function iU(t,n){return[...n.path,t]}function vI(t,n,e=mb){pb(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),oU(t,n),sU(t,n),aU(t,n),rU(t,n)}function yI(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Tm(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Nm(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function rU(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function pb(t,n){let e=RI(t);n.validator!==null?t.setValidators(pI(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=OI(t);n.asyncValidator!==null?t.setAsyncValidators(pI(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Nm(n._rawValidators,r),Nm(n._rawAsyncValidators,r)}function Tm(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=RI(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=OI(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(a=>a!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Nm(n._rawValidators,i),Nm(n._rawAsyncValidators,i),e}function oU(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&VI(t,n)})}function aU(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&VI(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function VI(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function sU(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function jI(t,n){t==null,pb(t,n)}function cU(t,n){return Tm(t,n)}function lU(t,n){if(!Object.hasOwn(t,"model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function dU(t){return Object.getPrototypeOf(t.constructor)===DI}function BI(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function uU(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Tn?e=o:dU(o)?i=o:r=o}),r||i||e||null}function fU(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var hU={provide:nU,useFactory:()=>{let t=u(hi,{self:!0});return{setParseErrors:n=>{t.setParseErrorSource(n)},set onReset(n){t.onReset=n}}}},hi=class extends Em{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof lc&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=uU(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(Et)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(De);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new ue,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof lc&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=XB(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof dc))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let a=this._convertErrors(o);n.setInputOnDirectives("errors",a)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&JB(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new cb({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=ct(()=>{let r=n();return r.length===0?null:r.reduce((o,a)=>(o[a.kind]=a,o),{})});this.parseErrorsValidator=(()=>e).bind(this),Vn(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}},km=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var mi=(()=>{class t extends km{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ie(hi,2))};static \u0275dir=P({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&z("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Be]})}return t})(),Pm=(()=>{class t extends km{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ie(sr,10))};static \u0275dir=P({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&z("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[Be]})}return t})(),Am=class extends Im{constructor(n,e,i){super(PI(e),FI(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){Se(()=>{QB(this,!0,n),Object.keys(n).forEach(i=>{ZB(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,re(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new lc(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return LI(this.controls,n)?this.controls[n]:null}};var mU={provide:sr,useExisting:Xt(()=>Ri)},Vd=Promise.resolve(),Ri=(()=>{class t extends sr{callSetDisabledState;get submitted(){return Se(this.submittedReactive)}_submitted=ct(()=>this.submittedReactive());submittedReactive=S(!1);_directives=new Set;form;ngSubmit=new A;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Am({},fb(e),hb(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Vd.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Vd.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Vd.then(()=>{let i=this._findContainer(e.path),r=new Am({});jI(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Vd.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Vd.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),BI(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Mm(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ie(Lr,10),ie(ub,10),ie(Om,8))};static \u0275dir=P({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&I("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Re([mU]),Be]})}return t})();function bI(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function CI(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var UI=class extends Im{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(PI(e),FI(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Rm(e)&&(e.nonNullable||e.initialValueIsDefault)&&(CI(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){Se(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new lc(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){bI(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){bI(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){CI(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var pU=t=>t instanceof UI;var gU=(()=>{class t extends sr{callSetDisabledState;get submitted(){return Se(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=ct(()=>this._submittedReactive());_submittedReactive=S(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),Object.hasOwn(e,"form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Tm(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){yI(e.control||null,e,!1),fU(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,BI(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Mm(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(yI(i||null,e),pU(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);jI(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&cU(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){pb(this.form,this),this._oldForm&&Tm(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(ie(Lr,10),ie(ub,10),ie(Om,8))};static \u0275dir=P({type:t,features:[Be,Pe]})}return t})(),_U={provide:sr,useExisting:Xt(()=>Ha)},Ha=(()=>{class t extends gU{form=null;ngSubmit=new A;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=en(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&I("submit",function(a){return r.onSubmit(a)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Re([_U]),Be]})}return t})(),vU={provide:hi,useExisting:Xt(()=>Gn)},wI=Promise.resolve(),Gn=(()=>{class t extends hi{_changeDetectorRef;callSetDisabledState;control=new UI;static ngAcceptInputType_isDisabled;_registered=!1;_ngModelInjector;viewModel;name="";isDisabled;model;options;update=new A;constructor(e,i,r,o,a,s,c,l){super(c,l,o),this._changeDetectorRef=a,this.callSetDisabledState=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r)}ngOnChanges(e){if(this._registered,this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),lU(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){super.ngControlUpdate(e,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,vI(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(e){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,vI(this.control,this,e))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){wI.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&j(i);wI.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?iU(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(ie(sr,9),ie(Lr,10),ie(ub,10),ie(xo,10),ie(De,8),ie(Om,8),ie(te,8),ie(Te,8))};static \u0275dir=P({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Re([vU,hU]),Be,Pe,ov(null)]})}return t})();var Fm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})(),yU={provide:xo,useExisting:Xt(()=>gb),multi:!0},gb=(()=>{class t extends DI{writeValue(e){let i=e??"";this.setProperty("value",i)}registerOnChange(e){this.onChange=i=>{e(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let e;return function(r){return(e||(e=en(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["input","type","number","formControlName","",3,"ngNoCva",""],["input","type","number","formControl","",3,"ngNoCva",""],["input","type","number","ngModel","",3,"ngNoCva",""]],hostBindings:function(i,r){i&1&&I("input",function(a){return r.onChange(a.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[Re([yU]),Be]})}return t})();var bU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({})}return t})();var pi=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Om,useValue:e.callSetDisabledState??mb}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[bU]})}return t})();var CU=["*"],HI=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:CU,decls:1,vars:0,template:function(i,r){i&1&&(ke(),se(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label, .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label, [dir=rtl] .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label, .mdc-form-field--align-end > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label, [dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2})}return t})();var wU=["input"],SU=["*"],_b={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},DU=new w("mat-checkbox-default-options",{providedIn:"root",factory:()=>_b}),on=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(on||{}),vb=class{source;checked},yb=(()=>{class t{_elementRef=u(U);_changeDetectorRef=u(De);_ngZone=u(H);_animationsDisabled=We();_options=u(DU,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new vb;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new A;indeterminateChange=new A;value;disableRipple=!1;_inputElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=on.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){u(Ge).load($n);let e=u(new ri("tabindex"),{optional:!0});this._options=this._options||_b,this.color=this._options.color||_b.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=u(Ye).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(on.Indeterminate):this._transitionCheckState(this.checked?on.Checked:on.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=S(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?on.Checked:on.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case on.Init:if(i===on.Checked)return this._animationClasses.uncheckedToChecked;if(i==on.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case on.Unchecked:return i===on.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case on.Checked:return i===on.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case on.Indeterminate:return i===on.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_preventBubblingFromLabel(e){e.target&&this._inputElement&&e.target!==this._inputElement.nativeElement&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&nt(wU,5),i&2){let o;K(o=Z())&&(r._inputElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(pt("id",r.id),ee("tabindex",null)("aria-label",null)("aria-labelledby",null),bt(r.color?"mat-"+r.color:"mat-accent"),z("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",j],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",j],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",j],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:ir(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",j],checked:[2,"checked","checked",j],disabled:[2,"disabled","disabled",j],indeterminate:[2,"indeterminate","indeterminate",j]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Re([{provide:xo,useExisting:Xt(()=>t),multi:!0},{provide:Lr,useExisting:t,multi:!0}]),Pe],ngContentSelectors:SU,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition","for"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-internal-form-field-label","mdc-label"]],template:function(i,r){if(i&1&&(ke(),m(0,"label",3),I("click",function(a){return r._preventBubblingFromLabel(a)}),m(1,"span",4,0),de(3,"span",5),m(4,"input",6,1),I("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(a){return r._onInteractionEvent(a)}),h(),de(6,"span",7),m(7,"span",8),yn(),m(8,"svg",9),de(9,"path",10),h(),ca(),de(10,"span",11),h(),de(11,"span",12),h(),m(12,"span",13,2),se(14),h()()),i&2){let o=ii(2);T("labelPosition",r.labelPosition)("for",r.inputId),g(4),z("mdc-checkbox--selected",r.checked),T("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),ee("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),g(7),T("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0)}},dependencies:[Ks,HI],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--%NS%mat-checkbox-state-layer-size, 40px);
  height: var(--%NS%mat-checkbox-state-layer-size, 40px);
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--%NS%mat-checkbox-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-hover-icon-color, var(--%NS%mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-focus-icon-color, var(--%NS%mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--%NS%mat-checkbox-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-checkbox-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-checkbox-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-checkbox-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-checkbox-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-checkbox-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
  color: var(--%NS%mat-checkbox-disabled-label-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
    color: GrayText;
  }
}
.mat-mdc-checkbox .mat-internal-form-field-label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-checkbox-touch-target-size, 48px);
  width: var(--%NS%mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})(),zI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[yb,Ne]})}return t})();var bb=class{_box;_destroyed=new N;_resizeSubject=new N;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new he(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(je(e=>e.some(i=>i.target===n)),Fu({bufferSize:1,refCount:!0}),Ee(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},$I=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=u(H);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new bb(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var xU=["notch"],MU=["*"],GI=["iconPrefixContainer"],WI=["textPrefixContainer"],qI=["iconSuffixContainer"],YI=["textSuffixContainer"],IU=["textField"],NU=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],TU=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function kU(t,n){t&1&&de(0,"span",21)}function AU(t,n){if(t&1&&(m(0,"label",20),se(1,1),R(2,kU,1,0,"span",21),h()),t&2){let e=D(2);T("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),ee("for",e._control.disableAutomaticLabeling?null:e._control.id),g(2),O(!e.hideRequiredMarker&&e._control.required?2:-1)}}function RU(t,n){if(t&1&&R(0,AU,3,5,"label",20),t&2){let e=D();O(e._hasFloatingLabel()?0:-1)}}function OU(t,n){t&1&&de(0,"div",7)}function PU(t,n){}function FU(t,n){if(t&1&&jn(0,PU,0,0,"ng-template",13),t&2){D(2);let e=ii(1);T("ngTemplateOutlet",e)}}function LU(t,n){if(t&1&&(m(0,"div",9),R(1,FU,1,1,null,13),h()),t&2){let e=D();T("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),g(),O(e._forceDisplayInfixLabel()?-1:1)}}function VU(t,n){t&1&&(m(0,"div",10,2),se(2,2),h())}function jU(t,n){t&1&&(m(0,"div",11,3),se(2,3),h())}function BU(t,n){}function UU(t,n){if(t&1&&jn(0,BU,0,0,"ng-template",13),t&2){D();let e=ii(1);T("ngTemplateOutlet",e)}}function HU(t,n){t&1&&(m(0,"div",14,4),se(2,4),h())}function zU(t,n){t&1&&(m(0,"div",15,5),se(2,5),h())}function $U(t,n){t&1&&de(0,"div",16)}function GU(t,n){t&1&&(m(0,"div",18),se(1,6),h())}function WU(t,n){if(t&1&&(m(0,"mat-hint",22),v(1),h()),t&2){let e=D(2);T("id",e._hintLabelId),g(),G(e.hintLabel)}}function qU(t,n){if(t&1&&(m(0,"div",19),R(1,WU,2,2,"mat-hint",22),se(2,7),de(3,"div",23),se(4,8),h()),t&2){let e=D();g(),O(e.hintLabel?1:-1)}}var Wn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-label"]]})}return t})(),YU=new w("MatError");var Cb=(()=>{class t{align="start";id=u(Ye).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(pt("id",r.id),ee("align",null),z("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),KU=new w("MatPrefix");var tN=new w("MatSuffix"),Vr=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[Re([{provide:tN,useExisting:t}])]})}return t})(),nN=new w("FloatingLabelParent"),KI=(()=>{class t{_elementRef=u(U);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u($I);_ngZone=u(H);_parent=u(nN);_resizeSubscription=new ue;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return ZU(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function ZU(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var ZI="mdc-line-ripple--active",Lm="mdc-line-ripple--deactivating",QI=(()=>{class t{_elementRef=u(U);_cleanupTransitionEnd;constructor(){let e=u(H),i=u(Te);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Lm),e.add(ZI)}deactivate(){this._elementRef.nativeElement.classList.add(Lm)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Lm);e.propertyName==="opacity"&&r&&i.remove(ZI,Lm)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),XI=(()=>{class t{_elementRef=u(U);_ngZone=u(H);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&nt(xU,5),i&2){let o;K(o=Z())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:MU,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(ke(),wn(0,"div",1),Ve(1,"div",2,0),se(3),$e(),wn(4,"div",3))},encapsulation:2})}return t})(),za=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t})}return t})();var $a=new w("MatFormField"),QU=new w("MAT_FORM_FIELD_DEFAULT_OPTIONS"),JI="fill",XU="auto",eN="fixed",JU="translateY(-50%)",gi=(()=>{class t{_elementRef=u(U);_changeDetectorRef=u(De);_platform=u(xe);_idGenerator=u(Ye);_ngZone=u(H);_defaults=u(QU,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Rl("iconPrefixContainer");_textPrefixContainerSignal=Rl("textPrefixContainer");_iconSuffixContainerSignal=Rl("iconSuffixContainer");_textSuffixContainerSignal=Rl("textSuffixContainer");_prefixSuffixContainers=ct(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=KD(Wn);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Pr(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||XU}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||JI;this._appearanceSignal.set(i)}_appearanceSignal=S(JI);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||eN}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||eN}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new N;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=We();constructor(){let e=this._defaults,i=u(Tt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Vn(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control,this._changeDetectorRef.markForCheck()),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=ct(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(dt([void 0,void 0]),ge(()=>[i.errorState,i.userAriaDescribedBy]),Pu(),je(([[o,a],[s,c]])=>o!==s||a!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Ee(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Rt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){pv({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=ct(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(a=>a&&!o.includes(a)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",f=`${a+s}px`,_=`calc(${d} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,b=`var(--mat-mdc-form-field-label-transform, ${JU} translateX(${_}))`,M=a+s+c+l;return[b,M]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Jf(o,r._labelChild,Wn,5),Wt(o,za,5)(o,KU,5)(o,tN,5)(o,YU,5)(o,Cb,5)),i&2){th();let a;K(a=Z())&&(r._formFieldControl=a.first),K(a=Z())&&(r._prefixChildren=a),K(a=Z())&&(r._suffixChildren=a),K(a=Z())&&(r._errorChildren=a),K(a=Z())&&(r._hintChildren=a)}},viewQuery:function(i,r){if(i&1&&(eh(r._iconPrefixContainerSignal,GI,5)(r._textPrefixContainerSignal,WI,5)(r._iconSuffixContainerSignal,qI,5)(r._textSuffixContainerSignal,YI,5),nt(IU,5)(GI,5)(WI,5)(qI,5)(YI,5)(KI,5)(XI,5)(QI,5)),i&2){th(4);let o;K(o=Z())&&(r._textField=o.first),K(o=Z())&&(r._iconPrefixContainer=o.first),K(o=Z())&&(r._textPrefixContainer=o.first),K(o=Z())&&(r._iconSuffixContainer=o.first),K(o=Z())&&(r._textSuffixContainer=o.first),K(o=Z())&&(r._floatingLabel=o.first),K(o=Z())&&(r._notchedOutline=o.first),K(o=Z())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&z("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Re([{provide:$a,useExisting:t},{provide:nN,useExisting:t}])],ngContentSelectors:TU,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(ke(NU),jn(0,RU,1,1,"ng-template",null,0,cv),m(2,"div",6,1),I("click",function(a){return r._control.onContainerClick(a)}),R(4,OU,1,0,"div",7),m(5,"div",8),R(6,LU,2,2,"div",9),R(7,VU,3,0,"div",10),R(8,jU,3,0,"div",11),m(9,"div",12),R(10,UU,1,1,null,13),se(11),h(),R(12,HU,3,0,"div",14),R(13,zU,3,0,"div",15),h(),R(14,$U,1,0,"div",16),h(),m(15,"div",17),R(16,GU,2,0,"div",18)(17,qU,5,1,"div",19),h()),i&2){let o;g(2),z("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),g(2),O(!r._hasOutline()&&!r._control.disabled?4:-1),g(2),O(r._hasOutline()?6:-1),g(),O(r._hasIconPrefix?7:-1),g(),O(r._hasTextPrefix?8:-1),g(2),O(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),g(2),O(r._hasTextSuffix?12:-1),g(),O(r._hasIconSuffix?13:-1),g(),O(r._hasOutline()?-1:14),g(),z("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let a=r._getSubscriptMessageType();g(),O((o=a)==="error"?16:o==="hint"?17:-1)}},dependencies:[KI,XI,bv,QI,Cb],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-filled-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-outlined-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-filled-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-outlined-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-top-right-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) {
  background-color: var(--%NS%mat-form-field-filled-container-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--%NS%mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-hover-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-filled-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-filled-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-filled-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-filled-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-outlined-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-outlined-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-outlined-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-outlined-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-outline-color, var(--%NS%mat-sys-outline));
  border-width: var(--%NS%mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-hover-outline-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-focus-outline-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-hover-outline-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-focus-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--%NS%mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--%NS%mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--%NS%mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-active-indicator-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-hover-active-indicator-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-active-indicator-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-hover-active-indicator-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--%NS%mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-focus-active-indicator-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-focus-active-indicator-color, var(--%NS%mat-sys-error));
}

.mdc-line-ripple--%NS%active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--%NS%deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--%NS%no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --%NS%mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--%NS%mat-form-field-container-height, 56px);
  padding-top: var(--%NS%mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--%NS%mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--%NS%mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--%NS%mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--%NS%mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--%NS%mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --%NS%mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--%NS%mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--%NS%mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--%NS%mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--%NS%mat-form-field-error-text-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-subscript-text-font, var(--%NS%mat-sys-body-small-font));
  line-height: var(--%NS%mat-form-field-subscript-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  font-size: var(--%NS%mat-form-field-subscript-text-size, var(--%NS%mat-sys-body-small-size));
  letter-spacing: var(--%NS%mat-form-field-subscript-text-tracking, var(--%NS%mat-sys-body-small-tracking));
  font-weight: var(--%NS%mat-form-field-subscript-text-weight, var(--%NS%mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--%NS%mat-form-field-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--%NS%mat-form-field-select-option-text-color, var(--%NS%mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--%NS%mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--%NS%mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--%NS%mat-form-field-enabled-select-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-focus-select-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --%NS%mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-container-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-form-field-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-form-field-container-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-form-field-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
  font-weight: var(--%NS%mat-form-field-container-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--%NS%mat-form-field-outlined-label-text-populated-size) * var(--%NS%mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--%NS%mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-trailing-icon-color, var(--%NS%mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-hover-trailing-icon-color, var(--%NS%mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-focus-trailing-icon-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--%NS%filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return t})();var gn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Kh,gi,Ne]})}return t})();var eH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),tH={passive:!0},iN=(()=>{class t{_platform=u(xe);_ngZone=u(H);_renderer=u(Mt).createRenderer(null,null);_styleLoader=u(Ge);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return vt;this._styleLoader.load(eH);let i=zn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new N,a="cdk-text-field-autofilled",s=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(a)?(i.classList.add(a),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(a)&&(i.classList.remove(a),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",s,tH)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=zn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var rN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({})}return t})();var fc=new w("");var oN=new w("MAT_INPUT_VALUE_ACCESSOR");var Mo=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}isSignalErrorState(e){if(!e)return!1;let i=e().invalid(),r=e().touched();return i&&r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var Io=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,i,r,o){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o,e?bn(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,e)??!1}};var nH=["button","checkbox","file","hidden","image","radio","range","reset","submit"],iH=new w("MAT_INPUT_CONFIG"),cr=(()=>{class t{_elementRef=u(U);_platform=u(xe);ngControl=u(hi,{optional:!0,self:!0});_autofillMonitor=u(iN);_ngZone=u(H);_formField=u($a,{optional:!0});_renderer=u(Te);_uid=u(Ye).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(iH,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new N;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=Pr(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Ai.required)??!1}set required(e){this._required=Pr(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Dy().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=Pr(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Dy().has(e));constructor(){let e=u(Ri,{optional:!0}),i=u(Ha,{optional:!0}),r=u(Mo),o=u(oN,{optional:!0,self:!0}),a=u(fc,{optional:!0,self:!0}),s=this._elementRef.nativeElement,c=s.nodeName.toLowerCase();o?bn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Io(r,a||this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=c==="select",this._isTextarea=c==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Vn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){nH.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&I("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(pt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),ee("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),z("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",j]},exportAs:["matInput"],features:[Re([{provide:za,useExisting:t}]),Pe]})}return t})(),lr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[gn,gn,rN,Ne]})}return t})();var zd=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new N;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var rH=20,Ga=(()=>{class t{_ngZone=u(H);_platform=u(xe);_renderer=u(Mt).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new N;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=rH){return this._platform.isBrowser?new he(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Au(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):X()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(je(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=zn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var oH=20,Br=(()=>{class t{_platform=u(xe);_listeners;_viewportSize=null;_change=new N;_document=u($);constructor(){let e=u(H),i=u(Mt).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,s=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:a,left:s}}change(e=oH){return e>0?this._change.pipe(Au(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var jr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({})}return t})(),wb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Ne,jr,Ne,jr]})}return t})();var $d=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Oi=class extends $d{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,a){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=a||null}},dr=class extends $d{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Sb=class extends $d{element;constructor(n){super(),this.element=n instanceof U?n.nativeElement:n}},hc=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Oi)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof dr)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Sb)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Gd=class extends hc{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(er,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||te.NULL,o=r.get(qe,i.injector);e=rh(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var mc=(()=>{class t extends hc{_moduleRef=u(er,{optional:!0});_document=u($);_viewContainerRef=u(Lt);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new A;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=(()=>{let e;return function(r){return(e||(e=en(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[Be]})}return t})(),Wd=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({})}return t})();var aN=iM();function $m(t){return new Vm(t.get(Br),t.get($))}var Vm=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=wt(-this._previousScrollPosition.left),n.style.top=wt(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",a=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),aN&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),aN&&(i.scrollBehavior=o,r.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function hN(t,n){return new jm(t.get(Ga),t.get(H),t.get(Br),n)}var jm=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(je(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var qd=class{enable(){}disable(){}attach(){}};function Db(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,a=t.left>e.right;return i||r||o||a})}function sN(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,a=t.right>e.right;return i||r||o||a})}function Fi(t,n){return new Bm(t.get(Ga),t.get(Br),t.get(H),n)}var Bm=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Db(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},mN=(()=>{class t{_injector=u(te);noop=()=>new qd;close=e=>hN(this._injector,e);block=()=>$m(this._injector);reposition=e=>Fi(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),Pi=class{positionStrategy;scrollStrategy=new qd;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Um=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var pN=(()=>{class t{_attachedOverlays=[];_document=u($);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),gN=(()=>{class t extends pN{_ngZone=u(H);_renderer=u(Mt).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),_N=(()=>{class t extends pN{_platform=u(xe);_ngZone=u(H);_renderer=u(Mt).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Yt(e)};_clickListener=e=>{let i=Yt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],c=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,c))){if(cN(s.overlayElement,i)||cN(s.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function cN(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var vN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),yN=(()=>{class t{_platform=u(xe);_containerElement;_document=u($);_styleLoader=u(Ge);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Sy()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Sy()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(vN)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),Eb=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function xb(t){return t&&t.nodeType===1}var Hm=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new N;_attachments=new N;_detachments=new N;_positionStrategy;_scrollStrategy;_locationChanges=ue.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new N;_outsidePointerEvents=new N;_afterNextRenderRef;constructor(n,e,i,r,o,a,s,c,l,d=!1,f,p){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=f,this._renderer=p,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=mt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=C(C({},this._config),n),this._updateElementSize()}setDirection(n){this._config=re(C({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=wt(this._config.width),n.height=wt(this._config.height),n.minWidth=wt(this._config.minWidth),n.minHeight=wt(this._config.minHeight),n.maxWidth=wt(this._config.maxWidth),n.maxHeight=wt(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;xb(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Eb(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Gs(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=mt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},lN="cdk-overlay-connected-position-bounding-box",aH=/([A-Za-z%]+)$/;function Hr(t,n){return new pc(n,t.get(Br),t.get($),t.get(xe),t.get(yN))}var pc=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new N;_resizeSubscription=ue.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(lN),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],a;for(let s of this._preferredPositions){let c=this._getOriginPoint(n,r,s),l=this._getOverlayPoint(c,e,s),d=this._getOverlayFit(l,e,i,s);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){o.push({position:s,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,s)});continue}(!a||a.overlayFit.visibleArea<d.visibleArea)&&(a={overlayFit:d,overlayPoint:l,originPoint:c,position:s,overlayRect:e})}if(o.length){let s=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,s=l)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(a.position,a.originPoint);return}this._applyPosition(a.position,a.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Wa(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(lN),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof U?this._origin.nativeElement:xb(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let a=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;r=i.originX=="start"?a:s}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=uN(e),{x:a,y:s}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(a+=c),l&&(s+=l);let d=0-a,f=a+o.width-i.width,p=0-s,_=s+o.height-i.height,b=this._subtractOverflows(o.width,d,f),M=this._subtractOverflows(o.height,p,_),x=b*M;return{visibleArea:x,isCompletelyWithinViewport:o.width*o.height===x,fitsInViewportVertically:M===o.height,fitsInViewportHorizontally:b==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,a=dN(this._overlayRef.getConfig().minHeight),s=dN(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||a!=null&&a<=r,l=n.fitsInViewportHorizontally||s!=null&&s<=o;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=uN(e),o=this._viewportRect,a=Math.max(n.x+r.width-o.width,0),s=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),d=0,f=0;return r.width<=o.width?d=l||-a:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=c||-s:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:f},{x:n.x+d,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!sH(this._lastScrollVisibility,i)){let r=new Um(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,a,s;if(e.overlayY==="top")a=n.y,o=i.height-a+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-s+this._getViewportMarginTop();else{let _=Math.min(i.bottom-n.y+i.top,n.y),b=this._lastBoundingBoxSize.height;o=_*2,a=n.y-_,o>b&&!this._isInitialRender&&!this._growAfterOpen&&(a=n.y-b/2)}let c=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,l=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,d,f,p;if(l)p=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)f=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let _=Math.min(i.right-n.x+i.left,n.x),b=this._lastBoundingBoxSize.width;d=_*2,f=n.x-_,d>b&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-b/2)}return{top:a,left:f,bottom:s,right:p,width:d,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,a=this._overlayRef.getConfig().maxWidth;r.width=wt(i.width),r.height=wt(i.height),r.top=wt(i.top)||"auto",r.bottom=wt(i.bottom)||"auto",r.left=wt(i.left)||"auto",r.right=wt(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=wt(o)),a&&(r.maxWidth=wt(a))}this._lastBoundingBoxSize=i,Wa(this._boundingBox.style,r)}_resetBoundingBoxStyles(){Wa(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Wa(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,a=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();Wa(i,this._getExactOverlayY(e,n,d)),Wa(i,this._getExactOverlayX(e,n,d))}else i.position="static";let s="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(s+=`translateX(${c}px) `),l&&(s+=`translateY(${l}px)`),i.transform=s.trim(),a.maxHeight&&(r?i.maxHeight=wt(a.maxHeight):o&&(i.maxHeight="")),a.maxWidth&&(r?i.maxWidth=wt(a.maxWidth):o&&(i.maxWidth="")),Wa(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let a=this._document.documentElement.clientHeight;r.bottom=`${a-(o.y+this._overlayRect.height)}px`}else r.top=wt(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let a;if(this._isRtl()?a=n.overlayX==="end"?"left":"right":a=n.overlayX==="end"?"right":"left",a==="right"){let s=this._document.documentElement.clientWidth;r.right=`${s-(o.x+this._overlayRect.width)}px`}else r.left=wt(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:sN(n,i),isOriginOutsideView:Db(n,i),isOverlayClipped:sN(e,i),isOverlayOutsideView:Db(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Gs(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof U)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function Wa(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function dN(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(aH);return!e||e==="px"?parseFloat(n):null}return t||null}function uN(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function sH(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var fN="cdk-global-overlay-wrapper";function _c(t){return new zm}var zm=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(fN),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:a,maxHeight:s}=i,c=(r==="100%"||r==="100vw")&&(!a||a==="100%"||a==="100vw"),l=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),d=this._xPosition,f=this._xOffset,p=this._overlayRef.getConfig().direction==="rtl",_="",b="",M="";c?M="flex-start":d==="center"?(M="center",p?b=f:_=f):p?d==="left"||d==="end"?(M="flex-end",_=f):(d==="right"||d==="start")&&(M="flex-start",b=f):d==="left"||d==="start"?(M="flex-start",_=f):(d==="right"||d==="end")&&(M="flex-end",b=f),n.position=this._cssPosition,n.marginLeft=c?"0":_,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":b,e.justifyContent=M,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(fN),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},bN=(()=>{class t{_injector=u(te);global(){return _c()}flexibleConnectedTo(e){return Hr(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),Yd=new w("OVERLAY_DEFAULT_CONFIG");function Li(t,n){t.get(Ge).load(vN);let e=t.get(yN),i=t.get($),r=t.get(Ye),o=t.get(Cn),a=t.get(Tt),s=t.get(Te,null,{optional:!0})||t.get(Mt).createRenderer(null,null),c=new Pi(n),l=t.get(Yd,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||a.value,!i.body||!("showPopover"in i.body)?c.usePopover=!1:c.usePopover=n?.usePopover??l;let d=i.createElement("div"),f=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),f.appendChild(d),c.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let p=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return xb(p)?p.after(f):p?.type==="parent"?p.element.appendChild(f):e.getContainerElement().appendChild(f),new Hm(new Gd(d,o,t),f,d,c,t.get(H),t.get(gN),i,t.get(ai),t.get(_N),n?.disableAnimations??t.get(cl,null,{optional:!0})==="NoopAnimations",t.get(qe),s)}var CN=(()=>{class t{scrollStrategies=u(mN);_positionBuilder=u(bN);_injector=u(te);create(e){return Li(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),cH=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],lH=new w("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(te);return()=>Fi(t)}}),gc=(()=>{class t{elementRef=u(U);static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),wN=new w("cdk-connected-overlay-default-config"),Gm=(()=>{class t{_dir=u(Tt,{optional:!0});_injector=u(te);_overlayRef;_templatePortal;_backdropSubscription=ue.EMPTY;_attachSubscription=ue.EMPTY;_detachSubscription=ue.EMPTY;_positionSubscription=ue.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=u(lH);_ngZone=u(H);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new A;positionChange=new A;attach=new A;detach=new A;overlayKeydown=new A;overlayOutsideClick=new A;constructor(){let e=u(un),i=u(Lt),r=u(wN,{optional:!0}),o=u(Yd,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new dr(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=cH);let e=this._overlayRef=Li(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!Ct(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Yt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Pi({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=Hr(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof gc?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof gc?this.origin.elementRef.nativeElement:this.origin instanceof U?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(kp(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",j],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",j],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",j],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",j],push:[2,"cdkConnectedOverlayPush","push",j],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",j],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",j],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Pe]})}return t})(),No=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({providers:[CN],imports:[Ne,Wd,wb,wb]})}return t})();var SN=(()=>{class t{_animationsDisabled=We();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&z("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--%NS%mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--%NS%mat-pseudo-checkbox-full-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--%NS%mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--%NS%mat-pseudo-checkbox-full-selected-icon-color, var(--%NS%mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-full-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return t})();var dH=["text"],uH=[[["mat-icon"]],"*"],fH=["mat-icon","*"];function hH(t,n){if(t&1&&de(0,"mat-pseudo-checkbox",1),t&2){let e=D();T("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function mH(t,n){if(t&1&&de(0,"mat-pseudo-checkbox",3),t&2){let e=D();T("disabled",e.disabled)}}function pH(t,n){if(t&1&&(m(0,"span",4),v(1),h()),t&2){let e=D();g(),pe("(",e.group.label,")")}}var Ib=new w("MAT_OPTION_PARENT_COMPONENT"),Nb=new w("MatOptgroup");var Mb=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},To=(()=>{class t{_element=u(U);_changeDetectorRef=u(De);_parent=u(Ib,{optional:!0});group=u(Nb,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=u(Ye).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=S(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new A;_text;_stateChanges=new N;constructor(){let e=u(Ge);e.load($n),e.load(Dn),this._signalDisableRipple=!!this._parent&&bn(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!Ct(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new Mb(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&nt(dH,7),i&2){let o;K(o=Z())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&I("click",function(){return r._selectViaInteraction()})("keydown",function(a){return r._handleKeydown(a)}),i&2&&(pt("id",r.id),ee("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),z("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",j]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:fH,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(ke(uH),R(0,hH,1,2,"mat-pseudo-checkbox",1),se(1),m(2,"span",2,0),se(4,1),h(),R(5,mH,1,1,"mat-pseudo-checkbox",3),R(6,pH,2,1,"span",4),de(7,"div",5)),i&2&&(O(r.multiple?0:-1),g(5),O(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),g(),O(r.group&&r.group._inert?6:-1),g(),T("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[SN,Ks],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--%NS%mat-option-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-option-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-option-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-option-label-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-option-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  font-weight: var(--%NS%mat-option-label-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--%NS%mat-option-hover-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--%NS%mat-option-focus-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--%NS%selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--%NS%mat-option-selected-state-layer-color, var(--%NS%mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--%NS%selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--%NS%mat-option-selected-state-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-option-selected-state-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--%NS%selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--%NS%selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --%NS%mat-list-list-item-selected-container-color: var(--%NS%mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})();function DN(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let a=0;a<t+1;a++)i[a].group&&i[a].group===r[o]&&o++;return o}return 0}function EN(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var xN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Ne]})}return t})();var Tb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Co,xN,To,Ne]})}return t})();var gH=["trigger"],_H=["panel"],vH=[[["mat-select-trigger"]],"*"],yH=["mat-select-trigger","*"];function bH(t,n){if(t&1&&(m(0,"span",4),v(1),h()),t&2){let e=D();g(),G(e.placeholder)}}function CH(t,n){t&1&&se(0)}function wH(t,n){if(t&1&&(m(0,"span",11),v(1),h()),t&2){let e=D(2);g(),G(e.triggerValue)}}function SH(t,n){if(t&1&&(m(0,"span",5),R(1,CH,1,0)(2,wH,2,1,"span",11),h()),t&2){let e=D();g(),O(e.customTrigger?1:2)}}function DH(t,n){if(t&1){let e=Ie();m(0,"div",12,1),I("keydown",function(r){q(e);let o=D();return Y(o._handleKeydown(r))}),se(2,1),h()}if(t&2){let e=D();bt(e.panelClass),z("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),ee("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var EH=new w("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(te);return()=>Fi(t)}}),xH=new w("MAT_SELECT_CONFIG"),MH=new w("MatSelectTrigger"),kb=class{source;value;constructor(n,e){this.source=n,this.value=e}},Wm=(()=>{class t{_viewportRuler=u(Br);_changeDetectorRef=u(De);_elementRef=u(U);_dir=u(Tt,{optional:!0});_idGenerator=u(Ye);_renderer=u(Te);_parentFormField=u($a,{optional:!0});ngControl=u(hi,{self:!0,optional:!0});_liveAnnouncer=u(md);_defaultOptions=u(xH,{optional:!0});_animationsDisabled=We();_popoverLocation;_initialized=new N;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=DN(e,this.options,this.optionGroups),a=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=EN(a.offsetTop,a.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new kb(this,e)}_scrollStrategyFactory=u(EH);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new N;_errorStateTracker;stateChanges=new N;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=S(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Ai.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=Go(()=>{let e=this.options;return e?e.changes.pipe(dt(e),ht(()=>Rt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(ht(()=>this.optionSelectionChanges))});openedChange=new A;_openedStream=this.openedChange.pipe(je(e=>e),ge(()=>{}));_closedStream=this.openedChange.pipe(je(e=>!e),ge(()=>{}));selectionChange=new A;valueChange=new A;constructor(){let e=u(Mo),i=u(Ri,{optional:!0}),r=u(Ha,{optional:!0}),o=u(new ri("tabindex"),{optional:!0}),a=u(Yd,{optional:!0}),s=u(fc,{optional:!0,self:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Io(e,s||this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=a?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new zd(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Ee(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Ee(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(dt(null),Ee(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Ot(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,a=this._keyManager;if(!a.isTyping()&&o&&!Ct(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let s=this.selected;a.onKeydown(e);let c=this.selected;c&&s!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,a=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!a&&(r===13||r===32)&&i.activeItem&&!Ct(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!a&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let s=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(s?c.select():c.deselect())})}else{let s=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==s&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!Ct(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof gc?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new vd(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Rt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Ee(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Rt(...this.options.map(i=>i._stateChanges)).pipe(Ee(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Yt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Wt(o,MH,5)(o,To,5)(o,Nb,5),i&2){let a;K(a=Z())&&(r.customTrigger=a.first),K(a=Z())&&(r.options=a),K(a=Z())&&(r.optionGroups=a)}},viewQuery:function(i,r){if(i&1&&nt(gH,5)(_H,5)(Gm,5),i&2){let o;K(o=Z())&&(r.trigger=o.first),K(o=Z())&&(r.panel=o.first),K(o=Z())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&I("keydown",function(a){return r._handleKeydown(a)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(ee("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),z("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",j],disableRipple:[2,"disableRipple","disableRipple",j],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:ir(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",j],placeholder:"placeholder",required:[2,"required","required",j],multiple:[2,"multiple","multiple",j],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",j],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",ir],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",j]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Re([{provide:za,useExisting:t},{provide:Ib,useExisting:t}]),Pe],ngContentSelectors:yH,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(ke(vH),m(0,"div",2,0),I("click",function(){return r.open()}),m(3,"div",3),R(4,bH,2,1,"span",4)(5,SH,3,1,"span",5),h(),m(6,"div",6)(7,"div",7),yn(),m(8,"svg",8),de(9,"path",9),h()()()(),jn(10,DH,3,16,"ng-template",10),I("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(a){return r._handleOverlayKeydown(a)})),i&2){let o=ii(1);g(3),ee("id",r._valueId),g(),O(r.empty?4:5),g(6),T("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[gc,Gm],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--%NS%mat-select-enabled-trigger-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-select-trigger-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-select-trigger-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-select-trigger-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-select-trigger-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-select-trigger-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--%NS%mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--%NS%mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--%NS%mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-select-invalid-arrow-color, var(--%NS%mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--%NS%mat-select-enabled-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--%NS%mat-select-focused-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--%NS%mat-select-disabled-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--%NS%mat-select-panel-background-color, var(--%NS%mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--%NS%mat-select-placeholder-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--%NS%mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2})}return t})();var qm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[No,Tb,Ne,jr,gn,Tb]})}return t})();var Ab=new w("MAT_DATE_LOCALE",{providedIn:"root",factory:()=>u(wa)}),vc="Method not implemented",Dt=class{locale;_localeChanges=new N;localeChanges=this._localeChanges;setTime(n,e,i,r){throw new Error(vc)}getHours(n){throw new Error(vc)}getMinutes(n){throw new Error(vc)}getSeconds(n){throw new Error(vc)}parseTime(n,e){throw new Error(vc)}addSeconds(n,e){throw new Error(vc)}getValidDateOrNull(n){return this.isDateInstance(n)&&this.isValid(n)?n:null}deserialize(n){return n==null||this.isDateInstance(n)&&this.isValid(n)?n:this.invalid()}setLocale(n){this.locale=n,this._localeChanges.next()}compareDate(n,e){return this.getYear(n)-this.getYear(e)||this.getMonth(n)-this.getMonth(e)||this.getDate(n)-this.getDate(e)}compareTime(n,e){return this.getHours(n)-this.getHours(e)||this.getMinutes(n)-this.getMinutes(e)||this.getSeconds(n)-this.getSeconds(e)}sameDate(n,e){if(n&&e){let i=this.isValid(n),r=this.isValid(e);return i&&r?!this.compareDate(n,e):i==r}return n==e}sameTime(n,e){if(n&&e){let i=this.isValid(n),r=this.isValid(e);return i&&r?!this.compareTime(n,e):i==r}return n==e}clampDate(n,e,i){return e&&this.compareDate(n,e)<0?e:i&&this.compareDate(n,i)>0?i:n}},ko=new w("mat-date-formats");var IH=["tooltip"],NH=20;var TH=new w("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(te);return()=>Fi(t,{scrollThrottle:NH})}}),kH=new w("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var IN="tooltip-panel",AH={passive:!0},RH=8,OH=8,PH=24,FH=200,NN=(()=>{class t{_elementRef=u(U);_ngZone=u(H);_platform=u(xe);_ariaDescriber=u(im);_focusMonitor=u(Ii);_dir=u(Tt);_injector=u(te);_viewContainerRef=u(Lt);_mediaMatcher=u(Ws);_document=u($);_renderer=u(Te);_animationsDisabled=We();_defaultOptions=u(kH,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=LH;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=Pr(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=Pr(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=$s(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=$s(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new N;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=RH}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(Ee(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new Oi(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(Ee(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let a=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&a._origin instanceof U)return this._overlayRef;this._detach()}let i=this._injector.get(Ga).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${IN}`,o=Hr(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(Ee(this._destroyed)).subscribe(a=>{this._updateCurrentPositionClass(a.connectionPair),this._tooltipInstance&&a.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=Li(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(TH)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(Ee(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(Ee(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(Ee(this._destroyed)).subscribe(a=>{a.preventDefault(),a.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(Ee(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(C(C({},r.main),o.main)),this._addOffset(C(C({},r.fallback),o.fallback))])}_addOffset(e){let i=OH,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:a}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:a}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:a}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:a}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),mt(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,a;if(i==="center"?this._dir&&this._dir.value==="rtl"?a=r==="end"?"left":"right":a=r==="start"?"left":"right":a=i==="bottom"&&o==="top"?"above":"below",a!==this._currentPosition){let s=this._overlayRef;if(s){let c=`${this._cssClassPrefix}-${IN}-`;s.removePanelClass(c+this._currentPosition),s.addPanelClass(c+a)}this._currentPosition=a}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,AH))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||mt({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!Ct(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),LH=(()=>{class t{_changeDetectorRef=u(De);_elementRef=u(U);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=We();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new N;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>PH&&e.width>=FH}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let a=getComputedStyle(i);(a.getPropertyValue("animation-duration")==="0s"||a.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&nt(IH,7),i&2){let o;K(o=Z())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&I("mouseleave",function(a){return r._handleMouseLeave(a)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(Ve(0,"div",1,0),ho("animationend",function(a){return r._handleAnimationEnd(a)}),Ve(2,"div",2),v(3),$e()()),i&2&&(bt(r.tooltipClass),z("mdc-tooltip--multiline",r._isMultiline),g(3),G(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--%NS%mat-tooltip-container-color, var(--%NS%mat-sys-inverse-surface));
  color: var(--%NS%mat-tooltip-supporting-text-color, var(--%NS%mat-sys-inverse-on-surface));
  border-radius: var(--%NS%mat-tooltip-container-shape, var(--%NS%mat-sys-corner-extra-small));
  font-family: var(--%NS%mat-tooltip-supporting-text-font, var(--%NS%mat-sys-body-small-font));
  font-size: var(--%NS%mat-tooltip-supporting-text-size, var(--%NS%mat-sys-body-small-size));
  font-weight: var(--%NS%mat-tooltip-supporting-text-weight, var(--%NS%mat-sys-body-small-weight));
  line-height: var(--%NS%mat-tooltip-supporting-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  letter-spacing: var(--%NS%mat-tooltip-supporting-text-tracking, var(--%NS%mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2})}return t})();function VH(t,n){return this._trackRow(n)}var VN=(t,n)=>n.id;function jH(t,n){if(t&1&&(Ve(0,"tr",0)(1,"td",3),v(2),$e()()),t&2){let e=D();g(),va("padding-top",e._cellPadding)("padding-bottom",e._cellPadding),ee("colspan",e.numCols),g(),pe(" ",e.label," ")}}function BH(t,n){if(t&1&&(Ve(0,"td",3),v(1),$e()),t&2){let e=D(2);va("padding-top",e._cellPadding)("padding-bottom",e._cellPadding),ee("colspan",e._firstRowOffset),g(),pe(" ",e._firstRowOffset>=e.labelMinRequiredCells?e.label:""," ")}}function UH(t,n){if(t&1){let e=Ie();Ve(0,"td",6)(1,"button",7),ho("click",function(r){let o=q(e).$implicit,a=D(2);return Y(a._cellClicked(o,r))})("focus",function(r){let o=q(e).$implicit,a=D(2);return Y(a._emitActiveDateChange(o,r))}),Ve(2,"span",8),v(3),$e(),wn(4,"span",9),$e()()}if(t&2){let e=n.$implicit,i=n.$index,r=D().$index,o=D();va("width",o._cellWidth)("padding-top",o._cellPadding)("padding-bottom",o._cellPadding),ee("data-mat-row",r)("data-mat-col",i),g(),bt(e.cssClasses),z("mat-calendar-body-disabled",!e.enabled)("mat-calendar-body-active",o._isActiveCell(r,i))("mat-calendar-body-range-start",o._isRangeStart(e.compareValue))("mat-calendar-body-range-end",o._isRangeEnd(e.compareValue))("mat-calendar-body-in-range",o._isInRange(e.compareValue))("mat-calendar-body-comparison-bridge-start",o._isComparisonBridgeStart(e.compareValue,r,i))("mat-calendar-body-comparison-bridge-end",o._isComparisonBridgeEnd(e.compareValue,r,i))("mat-calendar-body-comparison-start",o._isComparisonStart(e.compareValue))("mat-calendar-body-comparison-end",o._isComparisonEnd(e.compareValue))("mat-calendar-body-in-comparison-range",o._isInComparisonRange(e.compareValue))("mat-calendar-body-preview-start",o._isPreviewStart(e.compareValue))("mat-calendar-body-preview-end",o._isPreviewEnd(e.compareValue))("mat-calendar-body-in-preview",o._isInPreview(e.compareValue)),pt("tabIndex",o._isActiveCell(r,i)?0:-1),ee("aria-label",e.ariaLabel)("aria-disabled",!e.enabled||null)("aria-pressed",o._isSelected(e.compareValue))("aria-current",o.todayValue===e.compareValue?"date":null)("aria-describedby",o._getDescribedby(e.compareValue)),g(),z("mat-calendar-body-selected",o._isSelected(e.compareValue))("mat-calendar-body-comparison-identical",o._isComparisonIdentical(e.compareValue))("mat-calendar-body-today",o.todayValue===e.compareValue),g(),pe(" ",e.displayValue," ")}}function HH(t,n){if(t&1&&(Ve(0,"tr",1),R(1,BH,2,6,"td",4),at(2,UH,5,49,"td",5,VN),$e()),t&2){let e=n.$implicit,i=n.$index,r=D();g(),O(i===0&&r._firstRowOffset?1:-1),g(),st(e)}}function zH(t,n){if(t&1&&(m(0,"th",2)(1,"span",6),v(2),h(),m(3,"span",3),v(4),h()()),t&2){let e=n.$implicit;g(2),G(e.long),g(2),G(e.narrow)}}var $H=["*"];function GH(t,n){}function WH(t,n){if(t&1){let e=Ie();m(0,"mat-month-view",4),Es("activeDateChange",function(r){q(e);let o=D();return kl(o.activeDate,r)||(o.activeDate=r),Y(r)}),I("_userSelection",function(r){q(e);let o=D();return Y(o._dateSelected(r))})("dragStarted",function(r){q(e);let o=D();return Y(o._dragStarted(r))})("dragEnded",function(r){q(e);let o=D();return Y(o._dragEnded(r))}),h()}if(t&2){let e=D();Ds("activeDate",e.activeDate),T("selected",e.selected)("dateFilter",e.dateFilter)("maxDate",e.maxDate)("minDate",e.minDate)("dateClass",e.dateClass)("comparisonStart",e.comparisonStart)("comparisonEnd",e.comparisonEnd)("startDateAccessibleName",e.startDateAccessibleName)("endDateAccessibleName",e.endDateAccessibleName)("activeDrag",e._activeDrag)}}function qH(t,n){if(t&1){let e=Ie();m(0,"mat-year-view",5),Es("activeDateChange",function(r){q(e);let o=D();return kl(o.activeDate,r)||(o.activeDate=r),Y(r)}),I("monthSelected",function(r){q(e);let o=D();return Y(o._monthSelectedInYearView(r))})("selectedChange",function(r){q(e);let o=D();return Y(o._goToDateInView(r,"month"))}),h()}if(t&2){let e=D();Ds("activeDate",e.activeDate),T("selected",e.selected)("dateFilter",e.dateFilter)("maxDate",e.maxDate)("minDate",e.minDate)("dateClass",e.dateClass)}}function YH(t,n){if(t&1){let e=Ie();m(0,"mat-multi-year-view",6),Es("activeDateChange",function(r){q(e);let o=D();return kl(o.activeDate,r)||(o.activeDate=r),Y(r)}),I("yearSelected",function(r){q(e);let o=D();return Y(o._yearSelectedInMultiYearView(r))})("selectedChange",function(r){q(e);let o=D();return Y(o._goToDateInView(r,"year"))}),h()}if(t&2){let e=D();Ds("activeDate",e.activeDate),T("selected",e.selected)("dateFilter",e.dateFilter)("maxDate",e.maxDate)("minDate",e.minDate)("dateClass",e.dateClass)}}function KH(t,n){}var ZH=["button"],QH=[[["","matDatepickerToggleIcon",""]]],XH=["[matDatepickerToggleIcon]"];function JH(t,n){t&1&&(yn(),m(0,"svg",2),de(1,"path",3),h())}var ez=[[["input","matStartDate",""]],[["input","matEndDate",""]]],tz=["input[matStartDate]","input[matEndDate]"];var Cc=(()=>{class t{changes=new N;calendarLabel="Calendar";openCalendarLabel="Open calendar";closeCalendarLabel="Close calendar";prevMonthLabel="Previous month";nextMonthLabel="Next month";prevYearLabel="Previous year";nextYearLabel="Next year";prevMultiYearLabel="Previous 24 years";nextMultiYearLabel="Next 24 years";switchToMonthViewLabel="Choose date";switchToMultiYearViewLabel="Choose month and year";startDateLabel="Start date";endDateLabel="End date";comparisonDateLabel="Comparison range";formatYearRange(e,i){return`${e} \u2013 ${i}`}formatYearRangeLabel(e,i){return`${e} to ${i}`}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),nz=0,Zd=class{value;displayValue;ariaLabel;enabled;compareValue;rawValue;id=nz++;cssClasses;constructor(n,e,i,r,o,a=n,s){this.value=n,this.displayValue=e,this.ariaLabel=i,this.enabled=r,this.compareValue=a,this.rawValue=s,this.cssClasses=o instanceof Set?Array.from(o):o}},iz={passive:!1,capture:!0},Ym={passive:!0,capture:!0},TN={passive:!0},bc=(()=>{class t{_elementRef=u(U);_ngZone=u(H);_platform=u(xe);_intl=u(Cc);_eventCleanups;_skipNextFocus=!1;_focusActiveCellAfterViewChecked=!1;label;rows;todayValue;startValue;endValue;labelMinRequiredCells;numCols=7;activeCell=0;ngAfterViewChecked(){this._focusActiveCellAfterViewChecked&&(this._focusActiveCell(),this._focusActiveCellAfterViewChecked=!1)}isRange=!1;cellAspectRatio=1;comparisonStart=null;comparisonEnd=null;previewStart=null;previewEnd=null;startDateAccessibleName=null;endDateAccessibleName=null;selectedValueChange=new A;previewChange=new A;activeDateChange=new A;dragStarted=new A;dragEnded=new A;_firstRowOffset;_cellPadding;_cellWidth;_startDateLabelId;_endDateLabelId;_comparisonStartDateLabelId;_comparisonEndDateLabelId;_didDragSinceMouseDown=!1;_injector=u(te);comparisonDateAccessibleName=this._intl.comparisonDateLabel;_trackRow=e=>e;constructor(){let e=u(Te),i=u(Ye);this._startDateLabelId=i.getId("mat-calendar-body-start-"),this._endDateLabelId=i.getId("mat-calendar-body-end-"),this._comparisonStartDateLabelId=i.getId("mat-calendar-body-comparison-start-"),this._comparisonEndDateLabelId=i.getId("mat-calendar-body-comparison-end-"),u(Ge).load($n),this._ngZone.runOutsideAngular(()=>{let r=this._elementRef.nativeElement,o=[e.listen(r,"touchmove",this._touchmoveHandler,iz),e.listen(r,"mouseenter",this._enterHandler,Ym),e.listen(r,"focus",this._enterHandler,Ym),e.listen(r,"mouseleave",this._leaveHandler,Ym),e.listen(r,"blur",this._leaveHandler,Ym),e.listen(r,"mousedown",this._mousedownHandler,TN),e.listen(r,"touchstart",this._mousedownHandler,TN)];this._platform.isBrowser&&o.push(e.listen("window","mouseup",this._mouseupHandler),e.listen("window","touchend",this._touchendHandler)),this._eventCleanups=o})}_cellClicked(e,i){this._didDragSinceMouseDown||e.enabled&&this.selectedValueChange.emit({value:e.value,event:i})}_emitActiveDateChange(e,i){e.enabled&&this.activeDateChange.emit({value:e.value,event:i})}_isSelected(e){return this.startValue===e||this.endValue===e}ngOnChanges(e){let i=e.numCols,{rows:r,numCols:o}=this;(e.rows||i)&&(this._firstRowOffset=r&&r.length&&r[0].length?o-r[0].length:0),(e.cellAspectRatio||i||!this._cellPadding)&&(this._cellPadding=`${50*this.cellAspectRatio/o}%`),(i||!this._cellWidth)&&(this._cellWidth=`${100/o}%`)}ngOnDestroy(){this._eventCleanups.forEach(e=>e())}_isActiveCell(e,i){let r=e*this.numCols+i;return e&&(r-=this._firstRowOffset),r==this.activeCell}_focusActiveCell(e=!0){mt(()=>{setTimeout(()=>{let i=this._elementRef.nativeElement.querySelector(".mat-calendar-body-active");i&&(e||(this._skipNextFocus=!0),i.focus())})},{injector:this._injector})}_scheduleFocusActiveCellAfterViewChecked(){this._focusActiveCellAfterViewChecked=!0}_isRangeStart(e){return Pb(e,this.startValue,this.endValue)}_isRangeEnd(e){return Fb(e,this.startValue,this.endValue)}_isInRange(e){return Lb(e,this.startValue,this.endValue,this.isRange)}_isComparisonStart(e){return Pb(e,this.comparisonStart,this.comparisonEnd)}_isComparisonBridgeStart(e,i,r){if(!this._isComparisonStart(e)||this._isRangeStart(e)||!this._isInRange(e))return!1;let o=this.rows[i][r-1];if(!o){let a=this.rows[i-1];o=a&&a[a.length-1]}return o&&!this._isRangeEnd(o.compareValue)}_isComparisonBridgeEnd(e,i,r){if(!this._isComparisonEnd(e)||this._isRangeEnd(e)||!this._isInRange(e))return!1;let o=this.rows[i][r+1];if(!o){let a=this.rows[i+1];o=a&&a[0]}return o&&!this._isRangeStart(o.compareValue)}_isComparisonEnd(e){return Fb(e,this.comparisonStart,this.comparisonEnd)}_isInComparisonRange(e){return Lb(e,this.comparisonStart,this.comparisonEnd,this.isRange)}_isComparisonIdentical(e){return this.comparisonStart===this.comparisonEnd&&e===this.comparisonStart}_isPreviewStart(e){return Pb(e,this.previewStart,this.previewEnd)}_isPreviewEnd(e){return Fb(e,this.previewStart,this.previewEnd)}_isInPreview(e){return Lb(e,this.previewStart,this.previewEnd,this.isRange)}_getDescribedby(e){if(!this.isRange)return null;if(this.startValue===e&&this.endValue===e)return`${this._startDateLabelId} ${this._endDateLabelId}`;if(this.startValue===e)return this._startDateLabelId;if(this.endValue===e)return this._endDateLabelId;if(this.comparisonStart!==null&&this.comparisonEnd!==null){if(e===this.comparisonStart&&e===this.comparisonEnd)return`${this._comparisonStartDateLabelId} ${this._comparisonEndDateLabelId}`;if(e===this.comparisonStart)return this._comparisonStartDateLabelId;if(e===this.comparisonEnd)return this._comparisonEndDateLabelId}return null}_enterHandler=e=>{if(this._skipNextFocus&&e.type==="focus"){this._skipNextFocus=!1;return}if(e.target&&this.isRange){let i=this._getCellFromElement(e.target);i&&this._ngZone.run(()=>this.previewChange.emit({value:i.enabled?i:null,event:e}))}};_touchmoveHandler=e=>{if(!this.isRange)return;let i=kN(e),r=i?this._getCellFromElement(i):null;i!==e.target&&(this._didDragSinceMouseDown=!0),Ob(e.target)&&e.preventDefault(),this._ngZone.run(()=>this.previewChange.emit({value:r?.enabled?r:null,event:e}))};_leaveHandler=e=>{this.previewEnd!==null&&this.isRange&&(e.type!=="blur"&&(this._didDragSinceMouseDown=!0),e.target&&this._getCellFromElement(e.target)&&!(e.relatedTarget&&this._getCellFromElement(e.relatedTarget))&&this._ngZone.run(()=>this.previewChange.emit({value:null,event:e})))};_mousedownHandler=e=>{if(!this.isRange)return;this._didDragSinceMouseDown=!1;let i=e.target&&this._getCellFromElement(e.target);!i||!this._isInRange(i.compareValue)||this._ngZone.run(()=>{this.dragStarted.emit({value:i.rawValue,event:e})})};_mouseupHandler=e=>{if(!this.isRange)return;let i=Ob(e.target);if(!i){this._ngZone.run(()=>{this.dragEnded.emit({value:null,event:e})});return}i.closest(".mat-calendar-body")===this._elementRef.nativeElement&&this._ngZone.run(()=>{let r=this._getCellFromElement(i);this.dragEnded.emit({value:r?.rawValue??null,event:e})})};_touchendHandler=e=>{let i=kN(e);i&&this._mouseupHandler({target:i})};_getCellFromElement(e){let i=Ob(e);if(i){let r=i.getAttribute("data-mat-row"),o=i.getAttribute("data-mat-col");if(r&&o)return this.rows[parseInt(r)]?.[parseInt(o)]||null}return null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["","mat-calendar-body",""]],hostAttrs:[1,"mat-calendar-body"],inputs:{label:"label",rows:"rows",todayValue:"todayValue",startValue:"startValue",endValue:"endValue",labelMinRequiredCells:"labelMinRequiredCells",numCols:"numCols",activeCell:"activeCell",isRange:"isRange",cellAspectRatio:"cellAspectRatio",comparisonStart:"comparisonStart",comparisonEnd:"comparisonEnd",previewStart:"previewStart",previewEnd:"previewEnd",startDateAccessibleName:"startDateAccessibleName",endDateAccessibleName:"endDateAccessibleName"},outputs:{selectedValueChange:"selectedValueChange",previewChange:"previewChange",activeDateChange:"activeDateChange",dragStarted:"dragStarted",dragEnded:"dragEnded"},exportAs:["matCalendarBody"],features:[Pe],decls:11,vars:11,consts:[["aria-hidden","true"],["role","row"],[1,"mat-calendar-body-hidden-label",3,"id"],[1,"mat-calendar-body-label"],[1,"mat-calendar-body-label",3,"paddingTop","paddingBottom"],["role","gridcell",1,"mat-calendar-body-cell-container",3,"width","paddingTop","paddingBottom"],["role","gridcell",1,"mat-calendar-body-cell-container"],["type","button",1,"mat-calendar-body-cell",3,"click","focus","tabindex"],[1,"mat-calendar-body-cell-content","mat-focus-indicator"],["aria-hidden","true",1,"mat-calendar-body-cell-preview"]],template:function(i,r){i&1&&(R(0,jH,3,6,"tr",0),at(1,HH,4,1,"tr",1,VH,!0),Ve(3,"span",2),v(4),$e(),Ve(5,"span",2),v(6),$e(),Ve(7,"span",2),v(8),$e(),Ve(9,"span",2),v(10),$e()),i&2&&(O(r._firstRowOffset<r.labelMinRequiredCells?0:-1),g(),st(r.rows),g(2),pt("id",r._startDateLabelId),g(),pe(" ",r.startDateAccessibleName,`
`),g(),pt("id",r._endDateLabelId),g(),pe(" ",r.endDateAccessibleName,`
`),g(),pt("id",r._comparisonStartDateLabelId),g(),ya(" ",r.comparisonDateAccessibleName," ",r.startDateAccessibleName,`
`),g(),pt("id",r._comparisonEndDateLabelId),g(),ya(" ",r.comparisonDateAccessibleName," ",r.endDateAccessibleName,`
`))},styles:[`.mat-calendar-body {
  min-width: 224px;
}

.mat-calendar-body-today:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
  border-color: var(--%NS%mat-datepicker-calendar-date-today-outline-color, var(--%NS%mat-sys-primary));
}

.mat-calendar-body-label {
  height: 0;
  line-height: 0;
  text-align: start;
  padding-left: 4.7142857143%;
  padding-right: 4.7142857143%;
  font-size: var(--%NS%mat-datepicker-calendar-body-label-text-size, var(--%NS%mat-sys-title-small-size));
  font-weight: var(--%NS%mat-datepicker-calendar-body-label-text-weight, var(--%NS%mat-sys-title-small-weight));
  color: var(--%NS%mat-datepicker-calendar-body-label-text-color, var(--%NS%mat-sys-on-surface));
}

.mat-calendar-body-hidden-label {
  display: none;
}

.mat-calendar-body-cell-container {
  position: relative;
  height: 0;
  line-height: 0;
}

.mat-calendar-body-cell {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: none;
  text-align: center;
  outline: none;
  margin: 0;
  font-family: var(--%NS%mat-datepicker-calendar-text-font, var(--%NS%mat-sys-body-medium-font));
  font-size: var(--%NS%mat-datepicker-calendar-text-size, var(--%NS%mat-sys-body-medium-size));
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-calendar-body-cell::-moz-focus-inner {
  border: 0;
}

.mat-calendar-body-cell::before,
.mat-calendar-body-cell::after,
.mat-calendar-body-cell-preview {
  content: "";
  position: absolute;
  top: 5%;
  left: 0;
  z-index: 0;
  box-sizing: border-box;
  display: block;
  height: 90%;
  width: 100%;
}

.mat-calendar-body-range-start:not(.mat-calendar-body-in-comparison-range)::before,
.mat-calendar-body-range-start::after,
.mat-calendar-body-comparison-start:not(.mat-calendar-body-comparison-bridge-start)::before,
.mat-calendar-body-comparison-start::after,
.mat-calendar-body-preview-start .mat-calendar-body-cell-preview {
  left: 5%;
  width: 95%;
  border-top-left-radius: 999px;
  border-bottom-left-radius: 999px;
}
[dir=rtl] .mat-calendar-body-range-start:not(.mat-calendar-body-in-comparison-range)::before,
[dir=rtl] .mat-calendar-body-range-start::after,
[dir=rtl] .mat-calendar-body-comparison-start:not(.mat-calendar-body-comparison-bridge-start)::before,
[dir=rtl] .mat-calendar-body-comparison-start::after,
[dir=rtl] .mat-calendar-body-preview-start .mat-calendar-body-cell-preview {
  left: 0;
  border-radius: 0;
  border-top-right-radius: 999px;
  border-bottom-right-radius: 999px;
}

.mat-calendar-body-range-end:not(.mat-calendar-body-in-comparison-range)::before,
.mat-calendar-body-range-end::after,
.mat-calendar-body-comparison-end:not(.mat-calendar-body-comparison-bridge-end)::before,
.mat-calendar-body-comparison-end::after,
.mat-calendar-body-preview-end .mat-calendar-body-cell-preview {
  width: 95%;
  border-top-right-radius: 999px;
  border-bottom-right-radius: 999px;
}
[dir=rtl] .mat-calendar-body-range-end:not(.mat-calendar-body-in-comparison-range)::before,
[dir=rtl] .mat-calendar-body-range-end::after,
[dir=rtl] .mat-calendar-body-comparison-end:not(.mat-calendar-body-comparison-bridge-end)::before,
[dir=rtl] .mat-calendar-body-comparison-end::after,
[dir=rtl] .mat-calendar-body-preview-end .mat-calendar-body-cell-preview {
  left: 5%;
  border-radius: 0;
  border-top-left-radius: 999px;
  border-bottom-left-radius: 999px;
}

[dir=rtl] .mat-calendar-body-comparison-bridge-start.mat-calendar-body-range-end::after,
[dir=rtl] .mat-calendar-body-comparison-bridge-end.mat-calendar-body-range-start::after {
  width: 95%;
  border-top-right-radius: 999px;
  border-bottom-right-radius: 999px;
}

.mat-calendar-body-comparison-start.mat-calendar-body-range-end::after, [dir=rtl] .mat-calendar-body-comparison-start.mat-calendar-body-range-end::after,
.mat-calendar-body-comparison-end.mat-calendar-body-range-start::after,
[dir=rtl] .mat-calendar-body-comparison-end.mat-calendar-body-range-start::after {
  width: 90%;
}

.mat-calendar-body-in-preview {
  color: var(--%NS%mat-datepicker-calendar-date-preview-state-outline-color, var(--%NS%mat-sys-primary));
}
.mat-calendar-body-in-preview .mat-calendar-body-cell-preview {
  border-top: dashed 1px;
  border-bottom: dashed 1px;
}

.mat-calendar-body-preview-start .mat-calendar-body-cell-preview {
  border-left: dashed 1px;
}
[dir=rtl] .mat-calendar-body-preview-start .mat-calendar-body-cell-preview {
  border-left: 0;
  border-right: dashed 1px;
}

.mat-calendar-body-preview-end .mat-calendar-body-cell-preview {
  border-right: dashed 1px;
}
[dir=rtl] .mat-calendar-body-preview-end .mat-calendar-body-cell-preview {
  border-right: 0;
  border-left: dashed 1px;
}

.mat-calendar-body-disabled {
  cursor: default;
}
.mat-calendar-body-disabled > .mat-calendar-body-cell-content:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
  color: var(--%NS%mat-datepicker-calendar-date-disabled-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-calendar-body-disabled > .mat-calendar-body-today:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
  border-color: var(--%NS%mat-datepicker-calendar-date-today-disabled-state-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-calendar-body-disabled {
    opacity: 0.5;
  }
}

.mat-calendar-body-cell-content {
  top: 5%;
  left: 5%;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 90%;
  height: 90%;
  line-height: 1;
  border-width: 1px;
  border-style: solid;
  border-radius: 999px;
  color: var(--%NS%mat-datepicker-calendar-date-text-color, var(--%NS%mat-sys-on-surface));
  border-color: var(--%NS%mat-datepicker-calendar-date-outline-color, transparent);
}
.mat-calendar-body-cell-content.mat-focus-indicator {
  position: absolute;
}
.mat-calendar-body-cell-content::before {
  border-radius: 50%;
}
@media (forced-colors: active) {
  .mat-calendar-body-cell-content {
    border: none;
  }
}

.cdk-keyboard-focused .mat-calendar-body-active > .mat-calendar-body-cell-content:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical), .cdk-program-focused .mat-calendar-body-active > .mat-calendar-body-cell-content:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
  background-color: var(--%NS%mat-datepicker-calendar-date-focus-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-focus-state-layer-opacity) * 100%), transparent));
}

@media (hover: hover) {
  .mat-calendar-body-cell:not(.mat-calendar-body-disabled):hover > .mat-calendar-body-cell-content:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
    background-color: var(--%NS%mat-datepicker-calendar-date-hover-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-hover-state-layer-opacity) * 100%), transparent));
  }
}
.mat-calendar-body-selected {
  background-color: var(--%NS%mat-datepicker-calendar-date-selected-state-background-color, var(--%NS%mat-sys-primary));
  color: var(--%NS%mat-datepicker-calendar-date-selected-state-text-color, var(--%NS%mat-sys-on-primary));
}
.mat-calendar-body-disabled > .mat-calendar-body-selected {
  background-color: var(--%NS%mat-datepicker-calendar-date-selected-disabled-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-calendar-body-selected.mat-calendar-body-today {
  box-shadow: inset 0 0 0 1px var(--%NS%mat-datepicker-calendar-date-today-selected-state-outline-color, var(--%NS%mat-sys-primary));
}

.mat-calendar-body-in-range::before {
  background: var(--%NS%mat-datepicker-calendar-date-in-range-state-background-color, var(--%NS%mat-sys-primary-container));
}

.mat-calendar-body-comparison-identical,
.mat-calendar-body-in-comparison-range::before {
  background: var(--%NS%mat-datepicker-calendar-date-in-comparison-range-state-background-color, var(--%NS%mat-sys-tertiary-container));
}

.mat-calendar-body-comparison-identical,
.mat-calendar-body-in-comparison-range::before {
  background: var(--%NS%mat-datepicker-calendar-date-in-comparison-range-state-background-color, var(--%NS%mat-sys-tertiary-container));
}

.mat-calendar-body-comparison-bridge-start::before,
[dir=rtl] .mat-calendar-body-comparison-bridge-end::before {
  background: linear-gradient(to right, var(--%NS%mat-datepicker-calendar-date-in-range-state-background-color, var(--%NS%mat-sys-primary-container)) 50%, var(--%NS%mat-datepicker-calendar-date-in-comparison-range-state-background-color, var(--%NS%mat-sys-tertiary-container)) 50%);
}

.mat-calendar-body-comparison-bridge-end::before,
[dir=rtl] .mat-calendar-body-comparison-bridge-start::before {
  background: linear-gradient(to left, var(--%NS%mat-datepicker-calendar-date-in-range-state-background-color, var(--%NS%mat-sys-primary-container)) 50%, var(--%NS%mat-datepicker-calendar-date-in-comparison-range-state-background-color, var(--%NS%mat-sys-tertiary-container)) 50%);
}

.mat-calendar-body-in-range > .mat-calendar-body-comparison-identical,
.mat-calendar-body-in-comparison-range.mat-calendar-body-in-range::after {
  background: var(--%NS%mat-datepicker-calendar-date-in-overlap-range-state-background-color, var(--%NS%mat-sys-secondary-container));
}

.mat-calendar-body-comparison-identical.mat-calendar-body-selected,
.mat-calendar-body-in-comparison-range > .mat-calendar-body-selected {
  background: var(--%NS%mat-datepicker-calendar-date-in-overlap-range-selected-state-background-color, var(--%NS%mat-sys-secondary));
}

@media (forced-colors: active) {
  .mat-datepicker-popup:not(:empty),
  .mat-calendar-body-cell:not(.mat-calendar-body-in-range) .mat-calendar-body-selected {
    outline: solid 1px;
  }
  .mat-calendar-body-today {
    outline: dotted 1px;
  }
  .mat-calendar-body-cell::before,
  .mat-calendar-body-cell::after,
  .mat-calendar-body-selected {
    background: none;
  }
  .mat-calendar-body-in-range::before,
  .mat-calendar-body-comparison-bridge-start::before,
  .mat-calendar-body-comparison-bridge-end::before {
    border-top: solid 1px;
    border-bottom: solid 1px;
  }
  .mat-calendar-body-range-start::before {
    border-left: solid 1px;
  }
  [dir=rtl] .mat-calendar-body-range-start::before {
    border-left: 0;
    border-right: solid 1px;
  }
  .mat-calendar-body-range-end::before {
    border-right: solid 1px;
  }
  [dir=rtl] .mat-calendar-body-range-end::before {
    border-right: 0;
    border-left: solid 1px;
  }
  .mat-calendar-body-in-comparison-range::before {
    border-top: dashed 1px;
    border-bottom: dashed 1px;
  }
  .mat-calendar-body-comparison-start::before {
    border-left: dashed 1px;
  }
  [dir=rtl] .mat-calendar-body-comparison-start::before {
    border-left: 0;
    border-right: dashed 1px;
  }
  .mat-calendar-body-comparison-end::before {
    border-right: dashed 1px;
  }
  [dir=rtl] .mat-calendar-body-comparison-end::before {
    border-right: 0;
    border-left: dashed 1px;
  }
}
`],encapsulation:2})}return t})();function Rb(t){return t?.nodeName==="TD"}function Ob(t){let n;return Rb(t)?n=t:Rb(t.parentNode)?n=t.parentNode:Rb(t.parentNode?.parentNode)&&(n=t.parentNode.parentNode),n?.getAttribute("data-mat-row")!=null?n:null}function Pb(t,n,e){return e!==null&&n!==e&&t<e&&t===n}function Fb(t,n,e){return n!==null&&n!==e&&t>=n&&t===e}function Lb(t,n,e,i){return i&&n!==null&&e!==null&&n!==e&&t>=n&&t<=e}function kN(t){let n=t.changedTouches[0];return document.elementFromPoint(n.clientX,n.clientY)}var jt=class{start;end;_disableStructuralEquivalency;constructor(n,e){this.start=n,this.end=e}},Ao=(()=>{class t{selection;_adapter;_selectionChanged=new N;selectionChanged=this._selectionChanged;constructor(e,i){this.selection=e,this._adapter=i,this.selection=e}updateSelection(e,i){let r=this.selection;this.selection=e,this._selectionChanged.next({selection:e,source:i,oldValue:r})}ngOnDestroy(){this._selectionChanged.complete()}_isValidDateInstance(e){return this._adapter.isDateInstance(e)&&this._adapter.isValid(e)}static \u0275fac=function(i){ga()};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),rz=(()=>{class t extends Ao{constructor(e){super(null,e)}add(e){super.updateSelection(e,this)}isValid(){return this.selection!=null&&this._isValidDateInstance(this.selection)}isComplete(){return this.selection!=null}clone(){let e=new t(this._adapter);return e.updateSelection(this.selection,this),e}static \u0275fac=function(i){return new(i||t)(J(Dt))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),oz=(()=>{class t extends Ao{constructor(e){super(new jt(null,null),e)}add(e){let{start:i,end:r}=this.selection;i==null?i=e:r==null?r=e:(i=e,r=null),super.updateSelection(new jt(i,r),this)}isValid(){let{start:e,end:i}=this.selection;return e==null&&i==null?!0:e!=null&&i!=null?this._isValidDateInstance(e)&&this._isValidDateInstance(i)&&this._adapter.compareDate(e,i)<=0:(e==null||this._isValidDateInstance(e))&&(i==null||this._isValidDateInstance(i))}isComplete(){return this.selection.start!=null&&this.selection.end!=null}clone(){let e=new t(this._adapter);return e.updateSelection(this.selection,this),e}static \u0275fac=function(i){return new(i||t)(J(Dt))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),az={provide:Ao,useFactory:()=>u(Ao,{optional:!0,skipSelf:!0})||new rz(u(Dt))},sz={provide:Ao,useFactory:()=>u(Ao,{optional:!0,skipSelf:!0})||new oz(u(Dt))},Km=new w("MAT_DATE_RANGE_SELECTION_STRATEGY"),cz=(()=>{class t{_dateAdapter;constructor(e){this._dateAdapter=e}selectionFinished(e,i){let{start:r,end:o}=i;return r==null?r=e:o==null&&e&&this._dateAdapter.compareDate(e,r)>=0?o=e:(r=e,o=null),new jt(r,o)}createPreview(e,i){let r=null,o=null;return i.start&&!i.end&&e&&(r=i.start,o=e),new jt(r,o)}createDrag(e,i,r){let o=i.start,a=i.end;if(!o||!a)return null;let s=this._dateAdapter,c=s.compareDate(o,a)!==0,l=s.getYear(r)-s.getYear(e),d=s.getMonth(r)-s.getMonth(e),f=s.getDate(r)-s.getDate(e);return c&&s.sameDate(e,i.start)?(o=r,s.compareDate(r,a)>0&&(a=s.addCalendarYears(a,l),a=s.addCalendarMonths(a,d),a=s.addCalendarDays(a,f))):c&&s.sameDate(e,i.end)?(a=r,s.compareDate(r,o)<0&&(o=s.addCalendarYears(o,l),o=s.addCalendarMonths(o,d),o=s.addCalendarDays(o,f))):(o=s.addCalendarYears(o,l),o=s.addCalendarMonths(o,d),o=s.addCalendarDays(o,f),a=s.addCalendarYears(a,l),a=s.addCalendarMonths(a,d),a=s.addCalendarDays(a,f)),new jt(o,a)}static \u0275fac=function(i){return new(i||t)(J(Dt))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),Vb=7,lz=0,AN=(()=>{class t{_changeDetectorRef=u(De);_dateFormats=u(ko,{optional:!0});_dateAdapter=u(Dt,{optional:!0});_dir=u(Tt,{optional:!0});_rangeStrategy=u(Km,{optional:!0});_rerenderSubscription=ue.EMPTY;_selectionKeyPressed=!1;get activeDate(){return this._activeDate}set activeDate(e){let i=this._activeDate,r=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))||this._dateAdapter.today();this._activeDate=this._dateAdapter.clampDate(r,this.minDate,this.maxDate),this._hasSameMonthAndYear(i,this._activeDate)||this._init()}_activeDate;get selected(){return this._selected}set selected(e){e instanceof jt?this._selected=e:this._selected=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e)),this._setRanges(this._selected)}_selected=null;get minDate(){return this._minDate}set minDate(e){this._minDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_minDate=null;get maxDate(){return this._maxDate}set maxDate(e){this._maxDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_maxDate=null;dateFilter;dateClass;comparisonStart=null;comparisonEnd=null;startDateAccessibleName=null;endDateAccessibleName=null;activeDrag=null;selectedChange=new A;_userSelection=new A;dragStarted=new A;dragEnded=new A;activeDateChange=new A;_matCalendarBody;_monthLabel=S("");_weeks=S([]);_firstWeekOffset=S(0);_rangeStart=S(null);_rangeEnd=S(null);_comparisonRangeStart=S(null);_comparisonRangeEnd=S(null);_previewStart=S(null);_previewEnd=S(null);_isRange=S(!1);_todayDate=S(null);_weekdays=S([]);constructor(){u(Ge).load(Dn),this._activeDate=this._dateAdapter.today()}ngAfterContentInit(){this._rerenderSubscription=this._dateAdapter.localeChanges.pipe(dt(null)).subscribe(()=>this._init())}ngOnChanges(e){let i=e.comparisonStart||e.comparisonEnd;i&&!i.firstChange&&this._setRanges(this.selected),e.activeDrag&&!this.activeDrag&&this._clearPreview()}ngOnDestroy(){this._rerenderSubscription.unsubscribe()}_dateSelected(e){let i=e.value,r=this._getDateFromDayOfMonth(i),o,a;this._selected instanceof jt?(o=this._getDateInCurrentMonth(this._selected.start),a=this._getDateInCurrentMonth(this._selected.end)):o=a=this._getDateInCurrentMonth(this._selected),(o!==i||a!==i)&&this.selectedChange.emit(r),this._userSelection.emit({value:r,event:e.event}),this._clearPreview(),this._changeDetectorRef.markForCheck()}_updateActiveDate(e){let i=e.value,r=this._activeDate;this.activeDate=this._getDateFromDayOfMonth(i),this._dateAdapter.compareDate(r,this.activeDate)&&this.activeDateChange.emit(this._activeDate)}_handleCalendarBodyKeydown(e){let i=this._activeDate,r=this._isRtl();switch(e.keyCode){case 37:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,r?1:-1);break;case 39:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,r?-1:1);break;case 38:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,-7);break;case 40:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,7);break;case 36:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,1-this._dateAdapter.getDate(this._activeDate));break;case 35:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,this._dateAdapter.getNumDaysInMonth(this._activeDate)-this._dateAdapter.getDate(this._activeDate));break;case 33:this.activeDate=e.altKey?this._dateAdapter.addCalendarYears(this._activeDate,-1):this._dateAdapter.addCalendarMonths(this._activeDate,-1);break;case 34:this.activeDate=e.altKey?this._dateAdapter.addCalendarYears(this._activeDate,1):this._dateAdapter.addCalendarMonths(this._activeDate,1);break;case 13:case 32:this._selectionKeyPressed=!0,this._canSelect(this._activeDate)&&e.preventDefault();return;case 27:this._previewEnd()!=null&&!Ct(e)&&(this._clearPreview(),this.activeDrag?this.dragEnded.emit({value:null,event:e}):(this.selectedChange.emit(null),this._userSelection.emit({value:null,event:e})),e.preventDefault(),e.stopPropagation());return;default:return}this._dateAdapter.compareDate(i,this.activeDate)&&(this.activeDateChange.emit(this.activeDate),this._focusActiveCellAfterViewChecked()),e.preventDefault()}_handleCalendarBodyKeyup(e){(e.keyCode===32||e.keyCode===13)&&(this._selectionKeyPressed&&this._canSelect(this._activeDate)&&this._dateSelected({value:this._dateAdapter.getDate(this._activeDate),event:e}),this._selectionKeyPressed=!1)}_init(){this._setRanges(this.selected),this._todayDate.set(this._getCellCompareValue(this._dateAdapter.today())),this._monthLabel.set(this._dateFormats.display.monthLabel?this._dateAdapter.format(this.activeDate,this._dateFormats.display.monthLabel):this._dateAdapter.getMonthNames("short")[this._dateAdapter.getMonth(this.activeDate)].toLocaleUpperCase());let e=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),this._dateAdapter.getMonth(this.activeDate),1);this._firstWeekOffset.set((Vb+this._dateAdapter.getDayOfWeek(e)-this._dateAdapter.getFirstDayOfWeek())%Vb),this._initWeekdays(),this._createWeekCells(),this._changeDetectorRef.markForCheck()}_focusActiveCell(e){this._matCalendarBody._focusActiveCell(e)}_focusActiveCellAfterViewChecked(){this._matCalendarBody._scheduleFocusActiveCellAfterViewChecked()}_previewChanged({event:e,value:i}){if(this._rangeStrategy){let r=i?i.rawValue:null,o=this._rangeStrategy.createPreview(r,this.selected,e);if(this._previewStart.set(this._getCellCompareValue(o.start)),this._previewEnd.set(this._getCellCompareValue(o.end)),this.activeDrag&&r){let a=this._rangeStrategy.createDrag?.(this.activeDrag.value,this.selected,r,e);a&&(this._previewStart.set(this._getCellCompareValue(a.start)),this._previewEnd.set(this._getCellCompareValue(a.end)))}}}_dragEnded(e){if(this.activeDrag)if(e.value){let i=this._rangeStrategy?.createDrag?.(this.activeDrag.value,this.selected,e.value,e.event);this.dragEnded.emit({value:i??null,event:e.event})}else this.dragEnded.emit({value:null,event:e.event})}_getDateFromDayOfMonth(e){return this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),this._dateAdapter.getMonth(this.activeDate),e)}_initWeekdays(){let e=this._dateAdapter.getFirstDayOfWeek(),i=this._dateAdapter.getDayOfWeekNames("narrow"),o=this._dateAdapter.getDayOfWeekNames("long").map((a,s)=>({long:a,narrow:i[s],id:lz++}));this._weekdays.set(o.slice(e).concat(o.slice(0,e)))}_createWeekCells(){let e=this._dateAdapter.getNumDaysInMonth(this.activeDate),i=this._dateAdapter.getDateNames(),r=[[]];for(let o=0,a=this._firstWeekOffset();o<e;o++,a++){a==Vb&&(r.push([]),a=0);let s=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),this._dateAdapter.getMonth(this.activeDate),o+1),c=this._shouldEnableDate(s),l=this._dateAdapter.format(s,this._dateFormats.display.dateA11yLabel),d=this.dateClass?this.dateClass(s,"month"):void 0;r[r.length-1].push(new Zd(o+1,i[o],l,c,d,this._getCellCompareValue(s),s))}this._weeks.set(r)}_shouldEnableDate(e){return!!e&&(!this.minDate||this._dateAdapter.compareDate(e,this.minDate)>=0)&&(!this.maxDate||this._dateAdapter.compareDate(e,this.maxDate)<=0)&&(!this.dateFilter||this.dateFilter(e))}_getDateInCurrentMonth(e){return e&&this._hasSameMonthAndYear(e,this.activeDate)?this._dateAdapter.getDate(e):null}_hasSameMonthAndYear(e,i){return!!(e&&i&&this._dateAdapter.getMonth(e)==this._dateAdapter.getMonth(i)&&this._dateAdapter.getYear(e)==this._dateAdapter.getYear(i))}_getCellCompareValue(e){if(e){let i=this._dateAdapter.getYear(e),r=this._dateAdapter.getMonth(e),o=this._dateAdapter.getDate(e);return new Date(i,r,o).getTime()}return null}_isRtl(){return this._dir&&this._dir.value==="rtl"}_setRanges(e){e instanceof jt?(this._rangeStart.set(this._getCellCompareValue(e.start)),this._rangeEnd.set(this._getCellCompareValue(e.end)),this._isRange.set(!0)):(this._rangeStart.set(this._getCellCompareValue(e)),this._rangeEnd.set(this._rangeStart()),this._isRange.set(!1)),this._comparisonRangeStart.set(this._getCellCompareValue(this.comparisonStart)),this._comparisonRangeEnd.set(this._getCellCompareValue(this.comparisonEnd))}_canSelect(e){return!this.dateFilter||this.dateFilter(e)}_clearPreview(){this._previewStart.set(null),this._previewEnd.set(null)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-month-view"]],viewQuery:function(i,r){if(i&1&&nt(bc,5),i&2){let o;K(o=Z())&&(r._matCalendarBody=o.first)}},inputs:{activeDate:"activeDate",selected:"selected",minDate:"minDate",maxDate:"maxDate",dateFilter:"dateFilter",dateClass:"dateClass",comparisonStart:"comparisonStart",comparisonEnd:"comparisonEnd",startDateAccessibleName:"startDateAccessibleName",endDateAccessibleName:"endDateAccessibleName",activeDrag:"activeDrag"},outputs:{selectedChange:"selectedChange",_userSelection:"_userSelection",dragStarted:"dragStarted",dragEnded:"dragEnded",activeDateChange:"activeDateChange"},exportAs:["matMonthView"],features:[Pe],decls:8,vars:14,consts:[["role","grid",1,"mat-calendar-table"],[1,"mat-calendar-table-header"],["scope","col"],["aria-hidden","true"],["colspan","7",1,"mat-calendar-table-header-divider"],["mat-calendar-body","",3,"selectedValueChange","activeDateChange","previewChange","dragStarted","dragEnded","keyup","keydown","label","rows","todayValue","startValue","endValue","comparisonStart","comparisonEnd","previewStart","previewEnd","isRange","labelMinRequiredCells","activeCell","startDateAccessibleName","endDateAccessibleName"],[1,"cdk-visually-hidden"]],template:function(i,r){i&1&&(m(0,"table",0)(1,"thead",1)(2,"tr"),at(3,zH,5,2,"th",2,VN),h(),m(5,"tr",3),de(6,"th",4),h()(),m(7,"tbody",5),I("selectedValueChange",function(a){return r._dateSelected(a)})("activeDateChange",function(a){return r._updateActiveDate(a)})("previewChange",function(a){return r._previewChanged(a)})("dragStarted",function(a){return r.dragStarted.emit(a)})("dragEnded",function(a){return r._dragEnded(a)})("keyup",function(a){return r._handleCalendarBodyKeyup(a)})("keydown",function(a){return r._handleCalendarBodyKeydown(a)}),h()()),i&2&&(g(3),st(r._weekdays()),g(4),T("label",r._monthLabel())("rows",r._weeks())("todayValue",r._todayDate())("startValue",r._rangeStart())("endValue",r._rangeEnd())("comparisonStart",r._comparisonRangeStart())("comparisonEnd",r._comparisonRangeEnd())("previewStart",r._previewStart())("previewEnd",r._previewEnd())("isRange",r._isRange())("labelMinRequiredCells",3)("activeCell",r._dateAdapter.getDate(r.activeDate)-1)("startDateAccessibleName",r.startDateAccessibleName)("endDateAccessibleName",r.endDateAccessibleName))},dependencies:[bc],encapsulation:2})}return t})(),qn=24,jb=4,RN=(()=>{class t{_changeDetectorRef=u(De);_dateAdapter=u(Dt,{optional:!0});_dir=u(Tt,{optional:!0});_rerenderSubscription=ue.EMPTY;_selectionKeyPressed=!1;get activeDate(){return this._activeDate}set activeDate(e){let i=this._activeDate,r=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))||this._dateAdapter.today();this._activeDate=this._dateAdapter.clampDate(r,this.minDate,this.maxDate),jN(this._dateAdapter,i,this._activeDate,this.minDate,this.maxDate)||this._init()}_activeDate;get selected(){return this._selected}set selected(e){e instanceof jt?this._selected=e:this._selected=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e)),this._setSelectedYear(e)}_selected=null;get minDate(){return this._minDate}set minDate(e){this._minDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_minDate=null;get maxDate(){return this._maxDate}set maxDate(e){this._maxDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_maxDate=null;dateFilter;dateClass;selectedChange=new A;yearSelected=new A;activeDateChange=new A;_matCalendarBody;_years=S([]);_todayYear=S(0);_selectedYear=S(null);constructor(){this._dateAdapter,this._activeDate=this._dateAdapter.today()}ngAfterContentInit(){this._rerenderSubscription=this._dateAdapter.localeChanges.pipe(dt(null)).subscribe(()=>this._init())}ngOnDestroy(){this._rerenderSubscription.unsubscribe()}_init(){this._todayYear.set(this._dateAdapter.getYear(this._dateAdapter.today()));let i=this._dateAdapter.getYear(this._activeDate)-Kd(this._dateAdapter,this.activeDate,this.minDate,this.maxDate),r=[];for(let o=0,a=[];o<qn;o++)a.push(i+o),a.length==jb&&(r.push(a.map(s=>this._createCellForYear(s))),a=[]);this._years.set(r),this._changeDetectorRef.markForCheck()}_yearSelected(e){let i=e.value,r=this._dateAdapter.createDate(i,0,1),o=this._getDateFromYear(i);this.yearSelected.emit(r),this.selectedChange.emit(o)}_updateActiveDate(e){let i=e.value,r=this._activeDate;this.activeDate=this._getDateFromYear(i),this._dateAdapter.compareDate(r,this.activeDate)&&this.activeDateChange.emit(this.activeDate)}_handleCalendarBodyKeydown(e){let i=this._activeDate,r=this._isRtl();switch(e.keyCode){case 37:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,r?1:-1);break;case 39:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,r?-1:1);break;case 38:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,-jb);break;case 40:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,jb);break;case 36:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,-Kd(this._dateAdapter,this.activeDate,this.minDate,this.maxDate));break;case 35:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,qn-Kd(this._dateAdapter,this.activeDate,this.minDate,this.maxDate)-1);break;case 33:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,e.altKey?-qn*10:-qn);break;case 34:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,e.altKey?qn*10:qn);break;case 13:case 32:this._selectionKeyPressed=!0;break;default:return}this._dateAdapter.compareDate(i,this.activeDate)&&this.activeDateChange.emit(this.activeDate),this._focusActiveCellAfterViewChecked(),e.preventDefault()}_handleCalendarBodyKeyup(e){(e.keyCode===32||e.keyCode===13)&&(this._selectionKeyPressed&&this._yearSelected({value:this._dateAdapter.getYear(this._activeDate),event:e}),this._selectionKeyPressed=!1)}_getActiveCell(){return Kd(this._dateAdapter,this.activeDate,this.minDate,this.maxDate)}_focusActiveCell(){this._matCalendarBody._focusActiveCell()}_focusActiveCellAfterViewChecked(){this._matCalendarBody._scheduleFocusActiveCellAfterViewChecked()}_getDateFromYear(e){let i=this._dateAdapter.getMonth(this.activeDate),r=this._dateAdapter.getNumDaysInMonth(this._dateAdapter.createDate(e,i,1));return this._dateAdapter.createDate(e,i,Math.min(this._dateAdapter.getDate(this.activeDate),r))}_createCellForYear(e){let i=this._dateAdapter.createDate(e,0,1),r=this._dateAdapter.getYearName(i),o=this.dateClass?this.dateClass(i,"multi-year"):void 0;return new Zd(e,r,r,this._shouldEnableYear(e),o)}_shouldEnableYear(e){if(e==null||this.maxDate&&e>this._dateAdapter.getYear(this.maxDate)||this.minDate&&e<this._dateAdapter.getYear(this.minDate))return!1;if(!this.dateFilter)return!0;let i=this._dateAdapter.createDate(e,0,1);for(let r=i;this._dateAdapter.getYear(r)==e;r=this._dateAdapter.addCalendarDays(r,1))if(this.dateFilter(r))return!0;return!1}_isRtl(){return this._dir&&this._dir.value==="rtl"}_setSelectedYear(e){if(this._selectedYear.set(null),e instanceof jt){let i=e.start||e.end;i&&this._selectedYear.set(this._dateAdapter.getYear(i))}else e&&this._selectedYear.set(this._dateAdapter.getYear(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-multi-year-view"]],viewQuery:function(i,r){if(i&1&&nt(bc,5),i&2){let o;K(o=Z())&&(r._matCalendarBody=o.first)}},inputs:{activeDate:"activeDate",selected:"selected",minDate:"minDate",maxDate:"maxDate",dateFilter:"dateFilter",dateClass:"dateClass"},outputs:{selectedChange:"selectedChange",yearSelected:"yearSelected",activeDateChange:"activeDateChange"},exportAs:["matMultiYearView"],decls:5,vars:7,consts:[["role","grid",1,"mat-calendar-table"],["aria-hidden","true",1,"mat-calendar-table-header"],["colspan","4",1,"mat-calendar-table-header-divider"],["mat-calendar-body","",3,"selectedValueChange","activeDateChange","keyup","keydown","rows","todayValue","startValue","endValue","numCols","cellAspectRatio","activeCell"]],template:function(i,r){i&1&&(m(0,"table",0)(1,"thead",1)(2,"tr"),de(3,"th",2),h()(),m(4,"tbody",3),I("selectedValueChange",function(a){return r._yearSelected(a)})("activeDateChange",function(a){return r._updateActiveDate(a)})("keyup",function(a){return r._handleCalendarBodyKeyup(a)})("keydown",function(a){return r._handleCalendarBodyKeydown(a)}),h()()),i&2&&(g(4),T("rows",r._years())("todayValue",r._todayYear())("startValue",r._selectedYear())("endValue",r._selectedYear())("numCols",4)("cellAspectRatio",4/7)("activeCell",r._getActiveCell()))},dependencies:[bc],encapsulation:2})}return t})();function jN(t,n,e,i,r){let o=t.getYear(n),a=t.getYear(e),s=BN(t,i,r);return Math.floor((o-s)/qn)===Math.floor((a-s)/qn)}function Kd(t,n,e,i){let r=t.getYear(n);return dz(r-BN(t,e,i),qn)}function BN(t,n,e){let i=0;return e?i=t.getYear(e)-qn+1:n&&(i=t.getYear(n)),i}function dz(t,n){return(t%n+n)%n}var ON=(()=>{class t{_changeDetectorRef=u(De);_dateFormats=u(ko,{optional:!0});_dateAdapter=u(Dt,{optional:!0});_dir=u(Tt,{optional:!0});_rerenderSubscription=ue.EMPTY;_selectionKeyPressed=!1;get activeDate(){return this._activeDate}set activeDate(e){let i=this._activeDate,r=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))||this._dateAdapter.today();this._activeDate=this._dateAdapter.clampDate(r,this.minDate,this.maxDate),this._dateAdapter.getYear(i)!==this._dateAdapter.getYear(this._activeDate)&&this._init()}_activeDate;get selected(){return this._selected}set selected(e){e instanceof jt?this._selected=e:this._selected=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e)),this._setSelectedMonth(e)}_selected=null;get minDate(){return this._minDate}set minDate(e){this._minDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_minDate=null;get maxDate(){return this._maxDate}set maxDate(e){this._maxDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_maxDate=null;dateFilter;dateClass;selectedChange=new A;monthSelected=new A;activeDateChange=new A;_matCalendarBody;_months=S([]);_yearLabel=S("");_todayMonth=S(null);_selectedMonth=S(null);constructor(){this._activeDate=this._dateAdapter.today()}ngAfterContentInit(){this._rerenderSubscription=this._dateAdapter.localeChanges.pipe(dt(null)).subscribe(()=>this._init())}ngOnDestroy(){this._rerenderSubscription.unsubscribe()}_monthSelected(e){let i=e.value,r=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),i,1);this.monthSelected.emit(r);let o=this._getDateFromMonth(i);this.selectedChange.emit(o)}_updateActiveDate(e){let i=e.value,r=this._activeDate;this.activeDate=this._getDateFromMonth(i),this._dateAdapter.compareDate(r,this.activeDate)&&this.activeDateChange.emit(this.activeDate)}_handleCalendarBodyKeydown(e){let i=this._activeDate,r=this._isRtl();switch(e.keyCode){case 37:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,r?1:-1);break;case 39:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,r?-1:1);break;case 38:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,-4);break;case 40:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,4);break;case 36:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,-this._dateAdapter.getMonth(this._activeDate));break;case 35:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,11-this._dateAdapter.getMonth(this._activeDate));break;case 33:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,e.altKey?-10:-1);break;case 34:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,e.altKey?10:1);break;case 13:case 32:this._selectionKeyPressed=!0;break;default:return}this._dateAdapter.compareDate(i,this.activeDate)&&(this.activeDateChange.emit(this.activeDate),this._focusActiveCellAfterViewChecked()),e.preventDefault()}_handleCalendarBodyKeyup(e){(e.keyCode===32||e.keyCode===13)&&(this._selectionKeyPressed&&this._monthSelected({value:this._dateAdapter.getMonth(this._activeDate),event:e}),this._selectionKeyPressed=!1)}_init(){this._setSelectedMonth(this.selected),this._todayMonth.set(this._getMonthInCurrentYear(this._dateAdapter.today())),this._yearLabel.set(this._dateAdapter.getYearName(this.activeDate));let e=this._dateAdapter.getMonthNames("short");this._months.set([[0,1,2,3],[4,5,6,7],[8,9,10,11]].map(i=>i.map(r=>this._createCellForMonth(r,e[r])))),this._changeDetectorRef.markForCheck()}_focusActiveCell(){this._matCalendarBody._focusActiveCell()}_focusActiveCellAfterViewChecked(){this._matCalendarBody._scheduleFocusActiveCellAfterViewChecked()}_getMonthInCurrentYear(e){return e&&this._dateAdapter.getYear(e)==this._dateAdapter.getYear(this.activeDate)?this._dateAdapter.getMonth(e):null}_getDateFromMonth(e){let i=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),e,1),r=this._dateAdapter.getNumDaysInMonth(i);return this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),e,Math.min(this._dateAdapter.getDate(this.activeDate),r))}_createCellForMonth(e,i){let r=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),e,1),o=this._dateAdapter.format(r,this._dateFormats.display.monthYearA11yLabel),a=this.dateClass?this.dateClass(r,"year"):void 0;return new Zd(e,i.toLocaleUpperCase(),o,this._shouldEnableMonth(e),a)}_shouldEnableMonth(e){let i=this._dateAdapter.getYear(this.activeDate);if(e==null||this._isYearAndMonthAfterMaxDate(i,e)||this._isYearAndMonthBeforeMinDate(i,e))return!1;if(!this.dateFilter)return!0;let r=this._dateAdapter.createDate(i,e,1);for(let o=r;this._dateAdapter.getMonth(o)==e;o=this._dateAdapter.addCalendarDays(o,1))if(this.dateFilter(o))return!0;return!1}_isYearAndMonthAfterMaxDate(e,i){if(this.maxDate){let r=this._dateAdapter.getYear(this.maxDate),o=this._dateAdapter.getMonth(this.maxDate);return e>r||e===r&&i>o}return!1}_isYearAndMonthBeforeMinDate(e,i){if(this.minDate){let r=this._dateAdapter.getYear(this.minDate),o=this._dateAdapter.getMonth(this.minDate);return e<r||e===r&&i<o}return!1}_isRtl(){return this._dir&&this._dir.value==="rtl"}_setSelectedMonth(e){e instanceof jt?this._selectedMonth.set(this._getMonthInCurrentYear(e.start)||this._getMonthInCurrentYear(e.end)):this._selectedMonth.set(this._getMonthInCurrentYear(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-year-view"]],viewQuery:function(i,r){if(i&1&&nt(bc,5),i&2){let o;K(o=Z())&&(r._matCalendarBody=o.first)}},inputs:{activeDate:"activeDate",selected:"selected",minDate:"minDate",maxDate:"maxDate",dateFilter:"dateFilter",dateClass:"dateClass"},outputs:{selectedChange:"selectedChange",monthSelected:"monthSelected",activeDateChange:"activeDateChange"},exportAs:["matYearView"],decls:5,vars:9,consts:[["role","grid",1,"mat-calendar-table"],["aria-hidden","true",1,"mat-calendar-table-header"],["colspan","4",1,"mat-calendar-table-header-divider"],["mat-calendar-body","",3,"selectedValueChange","activeDateChange","keyup","keydown","label","rows","todayValue","startValue","endValue","labelMinRequiredCells","numCols","cellAspectRatio","activeCell"]],template:function(i,r){i&1&&(m(0,"table",0)(1,"thead",1)(2,"tr"),de(3,"th",2),h()(),m(4,"tbody",3),I("selectedValueChange",function(a){return r._monthSelected(a)})("activeDateChange",function(a){return r._updateActiveDate(a)})("keyup",function(a){return r._handleCalendarBodyKeyup(a)})("keydown",function(a){return r._handleCalendarBodyKeydown(a)}),h()()),i&2&&(g(4),T("label",r._yearLabel())("rows",r._months())("todayValue",r._todayMonth())("startValue",r._selectedMonth())("endValue",r._selectedMonth())("labelMinRequiredCells",2)("numCols",4)("cellAspectRatio",4/7)("activeCell",r._dateAdapter.getMonth(r.activeDate)))},dependencies:[bc],encapsulation:2})}return t})(),UN=(()=>{class t{_intl=u(Cc);calendar=u(Bb);_dateAdapter=u(Dt,{optional:!0});_dateFormats=u(ko,{optional:!0});_periodButtonText;_periodButtonDescription;_periodButtonLabel;_prevButtonLabel;_nextButtonLabel;constructor(){u(Ge).load(Dn);let e=u(De);this._updateLabels(),this.calendar.stateChanges.subscribe(()=>{this._updateLabels(),e.markForCheck()})}get periodButtonText(){return this._periodButtonText}get periodButtonDescription(){return this._periodButtonDescription}get periodButtonLabel(){return this._periodButtonLabel}get prevButtonLabel(){return this._prevButtonLabel}get nextButtonLabel(){return this._nextButtonLabel}currentPeriodClicked(){this.calendar.currentView=this.calendar.currentView=="month"?"multi-year":"month"}previousClicked(){this.previousEnabled()&&(this.calendar.activeDate=this.calendar.currentView=="month"?this._dateAdapter.addCalendarMonths(this.calendar.activeDate,-1):this._dateAdapter.addCalendarYears(this.calendar.activeDate,this.calendar.currentView=="year"?-1:-qn))}nextClicked(){this.nextEnabled()&&(this.calendar.activeDate=this.calendar.currentView=="month"?this._dateAdapter.addCalendarMonths(this.calendar.activeDate,1):this._dateAdapter.addCalendarYears(this.calendar.activeDate,this.calendar.currentView=="year"?1:qn))}previousEnabled(){return this.calendar.minDate?!this.calendar.minDate||!this._isSameView(this.calendar.activeDate,this.calendar.minDate):!0}nextEnabled(){return!this.calendar.maxDate||!this._isSameView(this.calendar.activeDate,this.calendar.maxDate)}_updateLabels(){let e=this.calendar,i=this._intl,r=this._dateAdapter;e.currentView==="month"?(this._periodButtonText=r.format(e.activeDate,this._dateFormats.display.monthYearLabel).toLocaleUpperCase(),this._periodButtonDescription=r.format(e.activeDate,this._dateFormats.display.monthYearLabel).toLocaleUpperCase(),this._periodButtonLabel=i.switchToMultiYearViewLabel,this._prevButtonLabel=i.prevMonthLabel,this._nextButtonLabel=i.nextMonthLabel):e.currentView==="year"?(this._periodButtonText=r.getYearName(e.activeDate),this._periodButtonDescription=r.getYearName(e.activeDate),this._periodButtonLabel=i.switchToMonthViewLabel,this._prevButtonLabel=i.prevYearLabel,this._nextButtonLabel=i.nextYearLabel):(this._periodButtonText=i.formatYearRange(...this._formatMinAndMaxYearLabels()),this._periodButtonDescription=i.formatYearRangeLabel(...this._formatMinAndMaxYearLabels()),this._periodButtonLabel=i.switchToMonthViewLabel,this._prevButtonLabel=i.prevMultiYearLabel,this._nextButtonLabel=i.nextMultiYearLabel)}_isSameView(e,i){return this.calendar.currentView=="month"?this._dateAdapter.getYear(e)==this._dateAdapter.getYear(i)&&this._dateAdapter.getMonth(e)==this._dateAdapter.getMonth(i):this.calendar.currentView=="year"?this._dateAdapter.getYear(e)==this._dateAdapter.getYear(i):jN(this._dateAdapter,e,i,this.calendar.minDate,this.calendar.maxDate)}_formatMinAndMaxYearLabels(){let i=this._dateAdapter.getYear(this.calendar.activeDate)-Kd(this._dateAdapter,this.calendar.activeDate,this.calendar.minDate,this.calendar.maxDate),r=i+qn-1,o=this._dateAdapter.getYearName(this._dateAdapter.createDate(i,0,1)),a=this._dateAdapter.getYearName(this._dateAdapter.createDate(r,0,1));return[o,a]}_periodButtonLabelId=u(Ye).getId("mat-calendar-period-label-");static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-calendar-header"]],exportAs:["matCalendarHeader"],ngContentSelectors:$H,decls:17,vars:13,consts:[[1,"mat-calendar-header"],[1,"mat-calendar-controls"],["aria-live","polite",1,"cdk-visually-hidden",3,"id"],["matButton","","type","button",1,"mat-calendar-period-button",3,"click"],["aria-hidden","true"],["viewBox","0 0 10 5","focusable","false","aria-hidden","true",1,"mat-calendar-arrow"],["points","0,0 5,5 10,0"],[1,"mat-calendar-spacer"],["matIconButton","","type","button","disabledInteractive","",1,"mat-calendar-previous-button",3,"click","disabled","matTooltip"],["viewBox","0 0 24 24","focusable","false","aria-hidden","true"],["d","M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"],["matIconButton","","type","button","disabledInteractive","",1,"mat-calendar-next-button",3,"click","disabled","matTooltip"],["d","M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"]],template:function(i,r){i&1&&(ke(),m(0,"div",0)(1,"div",1)(2,"span",2),v(3),h(),m(4,"button",3),I("click",function(){return r.currentPeriodClicked()}),m(5,"span",4),v(6),h(),yn(),m(7,"svg",5),de(8,"polygon",6),h()(),ca(),de(9,"div",7),se(10),m(11,"button",8),I("click",function(){return r.previousClicked()}),yn(),m(12,"svg",9),de(13,"path",10),h()(),ca(),m(14,"button",11),I("click",function(){return r.nextClicked()}),yn(),m(15,"svg",9),de(16,"path",12),h()()()()),i&2&&(g(2),T("id",r._periodButtonLabelId),g(),G(r.periodButtonDescription),g(),ee("aria-label",r.periodButtonLabel)("aria-describedby",r._periodButtonLabelId),g(2),G(r.periodButtonText),g(),z("mat-calendar-invert",r.calendar.currentView!=="month"),g(4),T("disabled",!r.previousEnabled())("matTooltip",r.prevButtonLabel),ee("aria-label",r.prevButtonLabel),g(3),T("disabled",!r.nextEnabled())("matTooltip",r.nextButtonLabel),ee("aria-label",r.nextButtonLabel))},dependencies:[St,or,NN],encapsulation:2})}return t})(),Bb=(()=>{class t{_dateAdapter=u(Dt,{optional:!0});_dateFormats=u(ko,{optional:!0});_changeDetectorRef=u(De);_elementRef=u(U);headerComponent;_calendarHeaderPortal;_intlChanges;_moveFocusOnNextTick=!1;get startAt(){return this._startAt}set startAt(e){this._startAt=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_startAt=null;startView="month";get selected(){return this._selected}set selected(e){e instanceof jt?this._selected=e:this._selected=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_selected=null;get minDate(){return this._minDate}set minDate(e){this._minDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_minDate=null;get maxDate(){return this._maxDate}set maxDate(e){this._maxDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_maxDate=null;dateFilter;dateClass;comparisonStart=null;comparisonEnd=null;startDateAccessibleName=null;endDateAccessibleName=null;selectedChange=new A;yearSelected=new A;monthSelected=new A;viewChanged=new A(!0);_userSelection=new A;_userDragDrop=new A;monthView;yearView;multiYearView;get activeDate(){return this._clampedActiveDate}set activeDate(e){this._clampedActiveDate=this._dateAdapter.clampDate(e,this.minDate,this.maxDate),this.stateChanges.next(),this._changeDetectorRef.markForCheck()}_clampedActiveDate;get currentView(){return this._currentView}set currentView(e){let i=this._currentView!==e?e:null;this._currentView=e,this._moveFocusOnNextTick=!0,this._changeDetectorRef.markForCheck(),i&&(this.stateChanges.next(),this.viewChanged.emit(i))}_currentView;_activeDrag=null;stateChanges=new N;constructor(){this._intlChanges=u(Cc).changes.subscribe(()=>{this._changeDetectorRef.markForCheck(),this.stateChanges.next()})}ngAfterContentInit(){this._calendarHeaderPortal=new Oi(this.headerComponent||UN),this.activeDate=this.startAt||this._dateAdapter.today(),this._currentView=this.startView}ngAfterViewChecked(){this._moveFocusOnNextTick&&(this._moveFocusOnNextTick=!1,this.focusActiveCell())}ngOnDestroy(){this._intlChanges.unsubscribe(),this.stateChanges.complete()}ngOnChanges(e){let i=e.minDate&&!this._dateAdapter.sameDate(e.minDate.previousValue,e.minDate.currentValue)?e.minDate:void 0,r=e.maxDate&&!this._dateAdapter.sameDate(e.maxDate.previousValue,e.maxDate.currentValue)?e.maxDate:void 0,o=i||r||e.dateFilter;if(o&&!o.firstChange){let a=this._getCurrentViewComponent();a&&(this._elementRef.nativeElement.contains(Hs())&&(this._moveFocusOnNextTick=!0),this._changeDetectorRef.detectChanges(),a._init())}this.stateChanges.next()}focusActiveCell(){this._getCurrentViewComponent()?._focusActiveCell(!1)}updateTodaysDate(){this._getCurrentViewComponent()?._init()}_dateSelected(e){let i=e.value;(this.selected instanceof jt||i&&!this._dateAdapter.sameDate(i,this.selected))&&this.selectedChange.emit(i),this._userSelection.emit(e)}_yearSelectedInMultiYearView(e){this.yearSelected.emit(e)}_monthSelectedInYearView(e){this.monthSelected.emit(e)}_goToDateInView(e,i){this.activeDate=e,this.currentView=i}_dragStarted(e){this._activeDrag=e}_dragEnded(e){this._activeDrag&&(e.value&&this._userDragDrop.emit(e),this._activeDrag=null)}_getCurrentViewComponent(){return this.monthView||this.yearView||this.multiYearView}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-calendar"]],viewQuery:function(i,r){if(i&1&&nt(AN,5)(ON,5)(RN,5),i&2){let o;K(o=Z())&&(r.monthView=o.first),K(o=Z())&&(r.yearView=o.first),K(o=Z())&&(r.multiYearView=o.first)}},hostAttrs:[1,"mat-calendar"],inputs:{headerComponent:"headerComponent",startAt:"startAt",startView:"startView",selected:"selected",minDate:"minDate",maxDate:"maxDate",dateFilter:"dateFilter",dateClass:"dateClass",comparisonStart:"comparisonStart",comparisonEnd:"comparisonEnd",startDateAccessibleName:"startDateAccessibleName",endDateAccessibleName:"endDateAccessibleName"},outputs:{selectedChange:"selectedChange",yearSelected:"yearSelected",monthSelected:"monthSelected",viewChanged:"viewChanged",_userSelection:"_userSelection",_userDragDrop:"_userDragDrop"},exportAs:["matCalendar"],features:[Re([az]),Pe],decls:5,vars:2,consts:[[3,"cdkPortalOutlet"],["cdkMonitorSubtreeFocus","","tabindex","-1",1,"mat-calendar-content"],[3,"activeDate","selected","dateFilter","maxDate","minDate","dateClass","comparisonStart","comparisonEnd","startDateAccessibleName","endDateAccessibleName","activeDrag"],[3,"activeDate","selected","dateFilter","maxDate","minDate","dateClass"],[3,"activeDateChange","_userSelection","dragStarted","dragEnded","activeDate","selected","dateFilter","maxDate","minDate","dateClass","comparisonStart","comparisonEnd","startDateAccessibleName","endDateAccessibleName","activeDrag"],[3,"activeDateChange","monthSelected","selectedChange","activeDate","selected","dateFilter","maxDate","minDate","dateClass"],[3,"activeDateChange","yearSelected","selectedChange","activeDate","selected","dateFilter","maxDate","minDate","dateClass"]],template:function(i,r){if(i&1&&(jn(0,GH,0,0,"ng-template",0),m(1,"div",1),R(2,WH,1,11,"mat-month-view",2)(3,qH,1,6,"mat-year-view",3)(4,YH,1,6,"mat-multi-year-view",3),h()),i&2){let o;T("cdkPortalOutlet",r._calendarHeaderPortal),g(2),O((o=r.currentView)==="month"?2:o==="year"?3:o==="multi-year"?4:-1)}},dependencies:[mc,Wh,AN,ON,RN],styles:[`.mat-calendar {
  display: block;
  line-height: normal;
  font-family: var(--%NS%mat-datepicker-calendar-text-font, var(--%NS%mat-sys-body-medium-font));
  font-size: var(--%NS%mat-datepicker-calendar-text-size, var(--%NS%mat-sys-body-medium-size));
}

.mat-calendar-header {
  padding: 8px 8px 0 8px;
}

.mat-calendar-content {
  padding: 0 8px 8px 8px;
  outline: none;
}

.mat-calendar-controls {
  display: flex;
  align-items: center;
  margin: 5% calc(4.7142857143% - 16px);
}

.mat-calendar-spacer {
  flex: 1 1 auto;
}

.mat-calendar-period-button {
  min-width: 0;
  margin: 0 8px;
  font-size: var(--%NS%mat-datepicker-calendar-period-button-text-size, var(--%NS%mat-sys-title-small-size));
  font-weight: var(--%NS%mat-datepicker-calendar-period-button-text-weight, var(--%NS%mat-sys-title-small-weight));
  --%NS%mat-button-text-label-text-color: var(--%NS%mat-datepicker-calendar-period-button-text-color, var(--%NS%mat-sys-on-surface-variant));
}

.mat-calendar-arrow {
  display: inline-block;
  width: 10px;
  height: 5px;
  margin: 0 0 0 5px;
  vertical-align: middle;
  fill: var(--%NS%mat-datepicker-calendar-period-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-calendar-arrow.mat-calendar-invert {
  transform: rotate(180deg);
}
[dir=rtl] .mat-calendar-arrow {
  margin: 0 5px 0 0;
}
@media (forced-colors: active) {
  .mat-calendar-arrow {
    fill: CanvasText;
  }
}

.mat-datepicker-content .mat-calendar-previous-button:not(.mat-mdc-button-disabled),
.mat-datepicker-content .mat-calendar-next-button:not(.mat-mdc-button-disabled) {
  color: var(--%NS%mat-datepicker-calendar-navigation-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-calendar-previous-button,
[dir=rtl] .mat-calendar-next-button {
  transform: rotate(180deg);
}

.mat-calendar-table {
  border-spacing: 0;
  border-collapse: collapse;
  width: 100%;
}

.mat-calendar-table-header th {
  text-align: center;
  padding: 0 0 8px 0;
  color: var(--%NS%mat-datepicker-calendar-header-text-color, var(--%NS%mat-sys-on-surface-variant));
  font-size: var(--%NS%mat-datepicker-calendar-header-text-size, var(--%NS%mat-sys-title-small-size));
  font-weight: var(--%NS%mat-datepicker-calendar-header-text-weight, var(--%NS%mat-sys-title-small-weight));
}

.mat-calendar-table-header-divider {
  position: relative;
  height: 1px;
}
.mat-calendar-table-header-divider::after {
  content: "";
  position: absolute;
  top: 0;
  left: -8px;
  right: -8px;
  height: 1px;
  background: var(--%NS%mat-datepicker-calendar-header-divider-color, transparent);
}

.mat-calendar-body-cell-content::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-calendar-body-cell:focus-visible .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})(),uz=new w("mat-datepicker-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(te);return()=>Fi(t)}}),HN=(()=>{class t{_elementRef=u(U);_animationsDisabled=We();_changeDetectorRef=u(De);_globalModel=u(Ao);_dateAdapter=u(Dt);_ngZone=u(H);_rangeSelectionStrategy=u(Km,{optional:!0});_stateChanges;_model;_eventCleanups;_animationFallback;_calendar;color;datepicker;comparisonStart=null;comparisonEnd=null;startDateAccessibleName=null;endDateAccessibleName=null;_isAbove=!1;_animationDone=new N;_isAnimating=!1;_closeButtonText;_closeButtonFocused=!1;_actionsPortal=null;_dialogLabelId=null;constructor(){if(u(Ge).load(Dn),this._closeButtonText=u(Cc).closeCalendarLabel,!this._animationsDisabled){let e=this._elementRef.nativeElement,i=u(Te);this._eventCleanups=this._ngZone.runOutsideAngular(()=>[i.listen(e,"animationstart",this._handleAnimationEvent),i.listen(e,"animationend",this._handleAnimationEvent),i.listen(e,"animationcancel",this._handleAnimationEvent)])}}ngAfterViewInit(){this._stateChanges=this.datepicker.stateChanges.subscribe(()=>{this._changeDetectorRef.markForCheck()}),this._calendar.focusActiveCell()}ngOnDestroy(){clearTimeout(this._animationFallback),this._eventCleanups?.forEach(e=>e()),this._stateChanges?.unsubscribe(),this._animationDone.complete()}_handleUserSelection(e){let i=this._model.selection,r=e.value,o=i instanceof jt;if(o&&this._rangeSelectionStrategy){let a=this._rangeSelectionStrategy.selectionFinished(r,i,e.event);this._model.updateSelection(a,this)}else r&&(o||!this._dateAdapter.sameDate(r,i))&&this._model.add(r);(!this._model||this._model.isComplete())&&!this._actionsPortal&&this.datepicker.close()}_handleUserDragDrop(e){this._model.updateSelection(e.value,this)}_startExitAnimation(){this._elementRef.nativeElement.classList.add("mat-datepicker-content-exit"),this._animationsDisabled?this._animationDone.next():(clearTimeout(this._animationFallback),this._animationFallback=setTimeout(()=>{this._isAnimating||this._animationDone.next()},200))}_handleAnimationEvent=e=>{let i=this._elementRef.nativeElement;e.target!==i||!e.animationName.startsWith("_mat-datepicker-content")||(clearTimeout(this._animationFallback),this._isAnimating=e.type==="animationstart",i.classList.toggle("mat-datepicker-content-animating",this._isAnimating),this._isAnimating||this._animationDone.next())};_getSelected(){return this._model.selection}_applyPendingSelection(){this._model!==this._globalModel&&this._globalModel.updateSelection(this._model.selection,this)}_assignActions(e,i){this._model=e?this._globalModel.clone():this._globalModel,this._actionsPortal=e,i&&this._changeDetectorRef.detectChanges()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-datepicker-content"]],viewQuery:function(i,r){if(i&1&&nt(Bb,5),i&2){let o;K(o=Z())&&(r._calendar=o.first)}},hostAttrs:[1,"mat-datepicker-content"],hostVars:6,hostBindings:function(i,r){i&2&&(bt(r.color?"mat-"+r.color:""),z("mat-datepicker-content-touch",r.datepicker.touchUi)("mat-datepicker-content-animations-enabled",!r._animationsDisabled))},inputs:{color:"color"},exportAs:["matDatepickerContent"],decls:5,vars:26,consts:[["cdkTrapFocus","","role","dialog",1,"mat-datepicker-content-container"],[3,"yearSelected","monthSelected","viewChanged","_userSelection","_userDragDrop","id","startAt","startView","minDate","maxDate","dateFilter","headerComponent","selected","dateClass","comparisonStart","comparisonEnd","startDateAccessibleName","endDateAccessibleName"],[3,"cdkPortalOutlet"],["type","button","matButton","elevated",1,"mat-datepicker-close-button",3,"focus","blur","click","color"]],template:function(i,r){i&1&&(m(0,"div",0)(1,"mat-calendar",1),I("yearSelected",function(a){return r.datepicker._selectYear(a)})("monthSelected",function(a){return r.datepicker._selectMonth(a)})("viewChanged",function(a){return r.datepicker._viewChanged(a)})("_userSelection",function(a){return r._handleUserSelection(a)})("_userDragDrop",function(a){return r._handleUserDragDrop(a)}),h(),jn(2,KH,0,0,"ng-template",2),m(3,"button",3),I("focus",function(){return r._closeButtonFocused=!0})("blur",function(){return r._closeButtonFocused=!1})("click",function(){return r.datepicker.close()}),v(4),h()()),i&2&&(z("mat-datepicker-content-container-with-custom-header",r.datepicker.calendarHeaderComponent)("mat-datepicker-content-container-with-actions",r._actionsPortal),ee("aria-modal",!0)("aria-labelledby",r._dialogLabelId??void 0),g(),bt(r.datepicker.panelClass),T("id",r.datepicker.id)("startAt",r.datepicker.startAt)("startView",r.datepicker.startView)("minDate",r.datepicker._getMinDate())("maxDate",r.datepicker._getMaxDate())("dateFilter",r.datepicker._getDateFilter())("headerComponent",r.datepicker.calendarHeaderComponent)("selected",r._getSelected())("dateClass",r.datepicker.dateClass)("comparisonStart",r.comparisonStart)("comparisonEnd",r.comparisonEnd)("startDateAccessibleName",r.startDateAccessibleName)("endDateAccessibleName",r.endDateAccessibleName),g(),T("cdkPortalOutlet",r._actionsPortal),g(),z("cdk-visually-hidden",!r._closeButtonFocused),T("color",r.color||"primary"),g(),G(r._closeButtonText))},dependencies:[by,Bb,mc,St],styles:[`@keyframes _mat-datepicker-content-dropdown-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-datepicker-content-dialog-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-datepicker-content-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-datepicker-content {
  display: block;
  background-color: var(--%NS%mat-datepicker-calendar-container-background-color, var(--%NS%mat-sys-surface-container-high));
  color: var(--%NS%mat-datepicker-calendar-container-text-color, var(--%NS%mat-sys-on-surface));
  box-shadow: var(--%NS%mat-datepicker-calendar-container-elevation-shadow, 0px 0px 0px 0px rgba(0, 0, 0, 0.2), 0px 0px 0px 0px rgba(0, 0, 0, 0.14), 0px 0px 0px 0px rgba(0, 0, 0, 0.12));
  border-radius: var(--%NS%mat-datepicker-calendar-container-shape, var(--%NS%mat-sys-corner-large));
}
.mat-datepicker-content.mat-datepicker-content-animations-enabled {
  animation: _mat-datepicker-content-dropdown-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-datepicker-content .mat-calendar {
  width: 296px;
  height: 354px;
}
.mat-datepicker-content .mat-datepicker-content-container-with-custom-header .mat-calendar {
  height: auto;
}
.mat-datepicker-content .mat-datepicker-close-button {
  position: absolute;
  top: 100%;
  left: 0;
  margin-top: 8px;
}
.mat-datepicker-content-animating .mat-datepicker-content .mat-datepicker-close-button {
  display: none;
}

.mat-datepicker-content-container {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}

.mat-datepicker-content-touch {
  display: block;
  max-height: 80vh;
  box-shadow: var(--%NS%mat-datepicker-calendar-container-touch-elevation-shadow, 0px 0px 0px 0px rgba(0, 0, 0, 0.2), 0px 0px 0px 0px rgba(0, 0, 0, 0.14), 0px 0px 0px 0px rgba(0, 0, 0, 0.12));
  border-radius: var(--%NS%mat-datepicker-calendar-container-touch-shape, var(--%NS%mat-sys-corner-extra-large));
  position: relative;
  overflow: visible;
  min-height: fit-content;
}
.mat-datepicker-content-touch.mat-datepicker-content-animations-enabled {
  animation: _mat-datepicker-content-dialog-enter 150ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-datepicker-content-touch .mat-datepicker-content-container {
  min-height: fit-content;
  max-height: 788px;
  min-width: 250px;
  max-width: 750px;
}
.mat-datepicker-content-touch .mat-calendar {
  width: 100%;
  height: auto;
}

.mat-datepicker-content-exit.mat-datepicker-content-animations-enabled {
  animation: _mat-datepicker-content-exit 100ms linear;
}

@media all and (orientation: landscape) {
  .mat-datepicker-content-touch .mat-datepicker-content-container {
    width: 64vh;
    height: 80vh;
  }
}
@media all and (orientation: portrait) {
  .mat-datepicker-content-touch .mat-datepicker-content-container {
    width: 80vw;
    height: 100vw;
  }
}
`],encapsulation:2})}return t})(),PN=(()=>{class t{_injector=u(te);_viewContainerRef=u(Lt);_dateAdapter=u(Dt,{optional:!0});_dir=u(Tt,{optional:!0});_model=u(Ao);_animationsDisabled=We();_scrollStrategy=u(uz);_inputStateChanges=ue.EMPTY;_document=u($);calendarHeaderComponent;get startAt(){return this._startAt||(this.datepickerInput?this.datepickerInput.getStartValue():null)}set startAt(e){this._startAt=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_startAt=null;startView="month";get color(){return this._color||(this.datepickerInput?this.datepickerInput.getThemePalette():void 0)}set color(e){this._color=e}_color;touchUi=!1;get disabled(){return this._disabled===void 0&&this.datepickerInput?this.datepickerInput.disabled:!!this._disabled}set disabled(e){e!==this._disabled&&(this._disabled=e,this.stateChanges.next(void 0))}_disabled;xPosition="start";yPosition="below";restoreFocus=!0;yearSelected=new A;monthSelected=new A;viewChanged=new A(!0);dateClass;openedStream=new A;closedStream=new A;get panelClass(){return this._panelClass}set panelClass(e){this._panelClass=sM(e)}_panelClass;get opened(){return this._opened}set opened(e){e?this.open():this.close()}_opened=!1;id=u(Ye).getId("mat-datepicker-");_getMinDate(){return this.datepickerInput&&this.datepickerInput.min}_getMaxDate(){return this.datepickerInput&&this.datepickerInput.max}_getDateFilter(){return this.datepickerInput&&this.datepickerInput.dateFilter}_overlayRef=null;_componentRef=null;_focusedElementBeforeOpen=null;_backdropHarnessClass=`${this.id}-backdrop`;_actionsPortal=null;datepickerInput;stateChanges=new N;_changeDetectorRef=u(De);constructor(){this._dateAdapter,this._model.selectionChanged.subscribe(()=>{this._changeDetectorRef.markForCheck()})}ngOnChanges(e){let i=e.xPosition||e.yPosition;if(i&&!i.firstChange&&this._overlayRef){let r=this._overlayRef.getConfig().positionStrategy;r instanceof pc&&(this._setConnectedPositions(r),this.opened&&this._overlayRef.updatePosition())}this.stateChanges.next(void 0)}ngOnDestroy(){this._destroyOverlay(),this.close(),this._inputStateChanges.unsubscribe(),this.stateChanges.complete()}select(e){this._model.add(e)}_selectYear(e){this.yearSelected.emit(e)}_selectMonth(e){this.monthSelected.emit(e)}_viewChanged(e){this.viewChanged.emit(e)}registerInput(e){return this.datepickerInput,this._inputStateChanges.unsubscribe(),this.datepickerInput=e,this._inputStateChanges=e.stateChanges.subscribe(()=>this.stateChanges.next(void 0)),this._model}registerActions(e){this._actionsPortal,this._actionsPortal=e,this._componentRef?.instance._assignActions(e,!0)}removeActions(e){e===this._actionsPortal&&(this._actionsPortal=null,this._componentRef?.instance._assignActions(null,!0))}open(){this._opened||this.disabled||this._componentRef?.instance._isAnimating||(this.datepickerInput,this._focusedElementBeforeOpen=Hs(),this._openOverlay(),this._opened=!0,this.openedStream.emit())}close(){if(!this._opened||this._componentRef?.instance._isAnimating)return;let e=this.restoreFocus&&this._focusedElementBeforeOpen&&typeof this._focusedElementBeforeOpen.focus=="function",i=()=>{this._opened&&(this._opened=!1,this.closedStream.emit())};if(this._componentRef){let{instance:r,location:o}=this._componentRef;r._animationDone.pipe(Ot(1)).subscribe(()=>{let a=this._document.activeElement;e&&(!a||a===this._document.activeElement||o.nativeElement.contains(a))&&this._focusedElementBeforeOpen.focus(),this._focusedElementBeforeOpen=null,this._destroyOverlay()}),r._startExitAnimation()}e?setTimeout(i):i()}_applyPendingSelection(){this._componentRef?.instance?._applyPendingSelection()}_forwardContentValues(e){e.datepicker=this,e.color=this.color,e._dialogLabelId=this.datepickerInput.getOverlayLabelId(),e._assignActions(this._actionsPortal,!1)}_openOverlay(){this._destroyOverlay();let e=this.touchUi,i=new Oi(HN,this._viewContainerRef),r=this._overlayRef=Li(this._injector,new Pi({positionStrategy:e?this._getDialogStrategy():this._getDropdownStrategy(),hasBackdrop:!0,backdropClass:[e?"cdk-overlay-dark-backdrop":"mat-overlay-transparent-backdrop",this._backdropHarnessClass],direction:this._dir||"ltr",scrollStrategy:e?$m(this._injector):this._scrollStrategy(),panelClass:`mat-datepicker-${e?"dialog":"popup"}`,disableAnimations:this._animationsDisabled}));this._getCloseStream(r).subscribe(o=>{o&&o.preventDefault(),this.close()}),r.keydownEvents().subscribe(o=>{let a=o.keyCode;(a===38||a===40||a===37||a===39||a===33||a===34)&&o.preventDefault()}),this._componentRef=r.attach(i),this._forwardContentValues(this._componentRef.instance),e||mt(()=>{r.updatePosition()},{injector:this._injector})}_destroyOverlay(){this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=this._componentRef=null)}_getDialogStrategy(){return _c(this._injector).centerHorizontally().centerVertically()}_getDropdownStrategy(){let e=Hr(this._injector,this.datepickerInput.getConnectedOverlayOrigin()).withTransformOriginOn(".mat-datepicker-content").withFlexibleDimensions(!1).withViewportMargin(8).withLockedPosition();return this._setConnectedPositions(e)}_setConnectedPositions(e){let i=this.xPosition==="end"?"end":"start",r=i==="start"?"end":"start",o=this.yPosition==="above"?"bottom":"top",a=o==="top"?"bottom":"top";return e.withPositions([{originX:i,originY:a,overlayX:i,overlayY:o},{originX:i,originY:o,overlayX:i,overlayY:a},{originX:r,originY:a,overlayX:r,overlayY:o},{originX:r,originY:o,overlayX:r,overlayY:a}])}_getCloseStream(e){let i=["ctrlKey","shiftKey","metaKey"];return Rt(e.backdropClick(),e.detachments(),e.keydownEvents().pipe(je(r=>r.keyCode===27&&!Ct(r)||this.datepickerInput&&Ct(r,"altKey")&&r.keyCode===38&&i.every(o=>!Ct(r,o)))))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,inputs:{calendarHeaderComponent:"calendarHeaderComponent",startAt:"startAt",startView:"startView",color:"color",touchUi:[2,"touchUi","touchUi",j],disabled:[2,"disabled","disabled",j],xPosition:"xPosition",yPosition:"yPosition",restoreFocus:[2,"restoreFocus","restoreFocus",j],dateClass:"dateClass",panelClass:"panelClass",opened:[2,"opened","opened",j]},outputs:{yearSelected:"yearSelected",monthSelected:"monthSelected",viewChanged:"viewChanged",openedStream:"opened",closedStream:"closed"},features:[Pe]})}return t})();var yc=class{target;targetElement;value=null;constructor(n,e){this.target=n,this.targetElement=e,this.value=this.target.value}},fz=(()=>{class t{_elementRef=u(U);_dateAdapter=u(Dt,{optional:!0});_dateFormats=u(ko,{optional:!0});_isInitialized=!1;get value(){return this._model?this._getValueFromModel(this._model.selection):this._pendingValue}set value(e){this._assignValueProgrammatically(e,!0)}_model;get disabled(){return!!this._disabled||this._parentDisabled()}set disabled(e){let i=e,r=this._elementRef.nativeElement;this._disabled!==i&&(this._disabled=i,this.stateChanges.next(void 0)),i&&this._isInitialized&&r.blur&&r.blur()}_disabled;dateChange=new A;dateInput=new A;stateChanges=new N;_onTouched=()=>{};_validatorOnChange=()=>{};_cvaOnChange=()=>{};_valueChangesSubscription=ue.EMPTY;_localeSubscription=ue.EMPTY;_pendingValue=null;_parseValidator=()=>this._lastValueValid?null:{matDatepickerParse:{text:this._elementRef.nativeElement.value}};_filterValidator=e=>{let i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e.value));return!i||this._matchesFilter(i)?null:{matDatepickerFilter:!0}};_minValidator=e=>{let i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e.value)),r=this._getMinDate();return!r||!i||this._dateAdapter.compareDate(r,i)<=0?null:{matDatepickerMin:{min:r,actual:i}}};_maxValidator=e=>{let i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e.value)),r=this._getMaxDate();return!r||!i||this._dateAdapter.compareDate(r,i)>=0?null:{matDatepickerMax:{max:r,actual:i}}};_getValidators(){return[this._parseValidator,this._minValidator,this._maxValidator,this._filterValidator]}_registerModel(e){this._model=e,this._valueChangesSubscription.unsubscribe(),this._pendingValue&&this._assignValue(this._pendingValue),this._valueChangesSubscription=this._model.selectionChanged.subscribe(i=>{if(this._shouldHandleChangeEvent(i)){let r=this._getValueFromModel(i.selection);this._lastValueValid=this._isValidValue(r),this._cvaOnChange(r),this._onTouched(),this._formatValue(r),this.dateInput.emit(new yc(this,this._elementRef.nativeElement)),this.dateChange.emit(new yc(this,this._elementRef.nativeElement))}})}_lastValueValid=!1;constructor(){this._localeSubscription=this._dateAdapter.localeChanges.subscribe(()=>{this._assignValueProgrammatically(this.value,!0)})}ngAfterViewInit(){this._isInitialized=!0}ngOnChanges(e){zN(e,this._dateAdapter)&&this.stateChanges.next(void 0)}ngOnDestroy(){this._valueChangesSubscription.unsubscribe(),this._localeSubscription.unsubscribe(),this.stateChanges.complete()}registerOnValidatorChange(e){this._validatorOnChange=e}validate(e){return this._validator?this._validator(e):null}writeValue(e){this._assignValueProgrammatically(e,e!==this.value)}registerOnChange(e){this._cvaOnChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}_onKeydown(e){let i=["ctrlKey","shiftKey","metaKey"];Ct(e,"altKey")&&e.keyCode===40&&i.every(o=>!Ct(e,o))&&!this._elementRef.nativeElement.readOnly&&(this._openPopup(),e.preventDefault())}_onInput(e){let i=e.target.value,r=this._lastValueValid,o=this._dateAdapter.parse(i,this._dateFormats.parse.dateInput);this._lastValueValid=this._isValidValue(o),o=this._dateAdapter.getValidDateOrNull(o);let a=!this._dateAdapter.sameDate(o,this.value);!o||a?this._cvaOnChange(o):(i&&!this.value&&this._cvaOnChange(o),r!==this._lastValueValid&&this._validatorOnChange()),a&&(this._assignValue(o),this.dateInput.emit(new yc(this,this._elementRef.nativeElement)))}_onChange(){this.dateChange.emit(new yc(this,this._elementRef.nativeElement))}_onBlur(){this.value&&this._formatValue(this.value),this._onTouched()}_formatValue(e){this._elementRef.nativeElement.value=e!=null?this._dateAdapter.format(e,this._dateFormats.display.dateInput):""}_assignValue(e){this._model?(this._assignValueToModel(e),this._pendingValue=null):this._pendingValue=e}_isValidValue(e){return!e||this._dateAdapter.isValid(e)}_parentDisabled(){return!1}_assignValueProgrammatically(e,i){e=this._dateAdapter.deserialize(e),this._lastValueValid=this._isValidValue(e),e=this._dateAdapter.getValidDateOrNull(e),this._assignValue(e),i&&this._formatValue(e)}_matchesFilter(e){let i=this._getDateFilter();return!i||i(e)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,inputs:{value:"value",disabled:[2,"disabled","disabled",j]},outputs:{dateChange:"dateChange",dateInput:"dateInput"},features:[Pe]})}return t})();function zN(t,n){let e=Object.keys(t);for(let i of e){let{previousValue:r,currentValue:o}=t[i];if(n.isDateInstance(r)&&n.isDateInstance(o)){if(!n.sameDate(r,o))return!0}else return!0}return!1}var hz=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matDatepickerToggleIcon",""]]})}return t})(),Hb=(()=>{class t{_intl=u(Cc);_changeDetectorRef=u(De);_stateChanges=ue.EMPTY;datepicker;tabIndex=null;ariaLabel;get disabled(){return this._disabled===void 0&&this.datepicker?this.datepicker.disabled:!!this._disabled}set disabled(e){this._disabled=e}_disabled;disableRipple=!1;_customIcon;_button;constructor(){let e=u(new ri("tabindex"),{optional:!0}),i=Number(e);this.tabIndex=i||i===0?i:null}ngOnChanges(e){e.datepicker&&this._watchStateChanges()}ngOnDestroy(){this._stateChanges.unsubscribe()}ngAfterContentInit(){this._watchStateChanges()}_open(e){this.datepicker&&!this.disabled&&(this.datepicker.open(),e.stopPropagation())}_watchStateChanges(){let e=this.datepicker?this.datepicker.stateChanges:X(),i=this.datepicker&&this.datepicker.datepickerInput?this.datepicker.datepickerInput.stateChanges:X(),r=this.datepicker?Rt(this.datepicker.openedStream,this.datepicker.closedStream):X();this._stateChanges.unsubscribe(),this._stateChanges=Rt(this._intl.changes,e,i,r).subscribe(()=>this._changeDetectorRef.markForCheck())}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-datepicker-toggle"]],contentQueries:function(i,r,o){if(i&1&&Wt(o,hz,5),i&2){let a;K(a=Z())&&(r._customIcon=a.first)}},viewQuery:function(i,r){if(i&1&&nt(ZH,5),i&2){let o;K(o=Z())&&(r._button=o.first)}},hostAttrs:[1,"mat-datepicker-toggle"],hostVars:8,hostBindings:function(i,r){i&1&&I("click",function(a){return r._open(a)}),i&2&&(ee("tabindex",null)("data-mat-calendar",r.datepicker?r.datepicker.id:null),z("mat-datepicker-toggle-active",r.datepicker&&r.datepicker.opened)("mat-accent",r.datepicker&&r.datepicker.color==="accent")("mat-warn",r.datepicker&&r.datepicker.color==="warn"))},inputs:{datepicker:[0,"for","datepicker"],tabIndex:"tabIndex",ariaLabel:[0,"aria-label","ariaLabel"],disabled:[2,"disabled","disabled",j],disableRipple:"disableRipple"},exportAs:["matDatepickerToggle"],features:[Pe],ngContentSelectors:XH,decls:4,vars:7,consts:[["button",""],["matIconButton","","type","button",3,"tabIndex","disabled","disableRipple"],["viewBox","0 0 24 24","width","24px","height","24px","fill","currentColor","focusable","false","aria-hidden","true",1,"mat-datepicker-toggle-default-icon"],["d","M19 3h-1V1h-2v2H8V1H6v2H5c-1.11 0-1.99.9-1.99 2L3 19c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 16H5V8h14v11zM7 10h5v5H7z"]],template:function(i,r){i&1&&(ke(QH),m(0,"button",1,0),R(2,JH,2,0,":svg:svg",2),se(3),h()),i&2&&(T("tabIndex",r.disabled?-1:r.tabIndex)("disabled",r.disabled)("disableRipple",r.disableRipple),ee("aria-haspopup",r.datepicker?"dialog":null)("aria-label",r.ariaLabel||r._intl.openCalendarLabel)("aria-expanded",r.datepicker?r.datepicker.opened:null),g(2),O(r._customIcon?-1:2))},dependencies:[or],styles:[`.mat-datepicker-toggle {
  pointer-events: auto;
  color: var(--%NS%mat-datepicker-toggle-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-datepicker-toggle button {
  color: inherit;
}

.mat-datepicker-toggle-active {
  color: var(--%NS%mat-datepicker-toggle-active-state-icon-color, var(--%NS%mat-sys-primary));
}

@media (forced-colors: active) {
  .mat-datepicker-toggle-default-icon {
    color: CanvasText;
  }
}
`],encapsulation:2})}return t})(),zb=(()=>{class t{_changeDetectorRef=u(De);_elementRef=u(U);_dateAdapter=u(Dt,{optional:!0});_formField=u($a,{optional:!0});_closedSubscription=ue.EMPTY;_openedSubscription=ue.EMPTY;_startInput;_endInput;get value(){return this._model?this._model.selection:null}id=u(Ye).getId("mat-date-range-input-");focused=!1;get shouldLabelFloat(){return this.focused||!this.empty}controlType="mat-date-range-input";get placeholder(){let e=this._startInput?._getPlaceholder()||"",i=this._endInput?._getPlaceholder()||"";return e||i?`${e} ${this.separator} ${i}`:""}get rangePicker(){return this._rangePicker}set rangePicker(e){e&&(this._model=e.registerInput(this),this._rangePicker=e,this._closedSubscription.unsubscribe(),this._openedSubscription.unsubscribe(),this._ariaOwns.set(this.rangePicker.opened?e.id:null),this._closedSubscription=e.closedStream.subscribe(()=>{this._startInput?._onTouched(),this._endInput?._onTouched(),this._ariaOwns.set(null)}),this._openedSubscription=e.openedStream.subscribe(()=>{this._ariaOwns.set(e.id)}),this._registerModel(this._model))}_rangePicker;_ariaOwns=S(null);get required(){return this._required??(this._isTargetRequired(this)||this._isTargetRequired(this._startInput)||this._isTargetRequired(this._endInput))??!1}set required(e){this._required=e}_required;get dateFilter(){return this._dateFilter}set dateFilter(e){let i=this._startInput,r=this._endInput,o=i&&i._matchesFilter(i.value),a=r&&r._matchesFilter(i.value);this._dateFilter=e,i&&i._matchesFilter(i.value)!==o&&i._validatorOnChange(),r&&r._matchesFilter(r.value)!==a&&r._validatorOnChange()}_dateFilter;get min(){return this._min}set min(e){let i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e));this._dateAdapter.sameDate(i,this._min)||(this._min=i,this._revalidate())}_min=null;get max(){return this._max}set max(e){let i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e));this._dateAdapter.sameDate(i,this._max)||(this._max=i,this._revalidate())}_max=null;get disabled(){return this._startInput&&this._endInput?this._startInput.disabled&&this._endInput.disabled:this._groupDisabled}set disabled(e){e!==this._groupDisabled&&(this._groupDisabled=e,this.stateChanges.next(void 0))}_groupDisabled=!1;get errorState(){return this._startInput&&this._endInput?this._startInput.errorState||this._endInput.errorState:!1}get empty(){let e=this._startInput?this._startInput.isEmpty():!1,i=this._endInput?this._endInput.isEmpty():!1;return e&&i}_ariaDescribedBy=null;_model;separator="\u2013";comparisonStart=null;comparisonEnd=null;ngControl;stateChanges=new N;disableAutomaticLabeling=!0;constructor(){this._dateAdapter,this._formField?._elementRef.nativeElement.classList.contains("mat-mdc-form-field")&&this._elementRef.nativeElement.classList.add("mat-mdc-input-element","mat-mdc-form-field-input-control","mdc-text-field__input"),this.ngControl=u(sr,{optional:!0,self:!0})}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){this._ariaDescribedBy=e.length?e.join(" "):null}onContainerClick(){!this.focused&&!this.disabled&&(!this._model||!this._model.selection.start?this._startInput.focus():this._endInput.focus())}ngAfterContentInit(){this._model&&this._registerModel(this._model),Rt(this._startInput.stateChanges,this._endInput.stateChanges).subscribe(()=>{this.stateChanges.next(void 0)})}ngOnChanges(e){zN(e,this._dateAdapter)&&this.stateChanges.next(void 0)}ngOnDestroy(){this._closedSubscription.unsubscribe(),this._openedSubscription.unsubscribe(),this.stateChanges.complete()}getStartValue(){return this.value?this.value.start:null}getThemePalette(){return this._formField?this._formField.color:void 0}getConnectedOverlayOrigin(){return this._formField?this._formField.getConnectedOverlayOrigin():this._elementRef}getOverlayLabelId(){return this._formField?this._formField.getLabelId():null}_getInputMirrorValue(e){let i=e==="start"?this._startInput:this._endInput;return i?i.getMirrorValue():""}_shouldHidePlaceholders(){return this._startInput?!this._startInput.isEmpty():!1}_handleChildValueChange(){this.stateChanges.next(void 0),this._changeDetectorRef.markForCheck()}_openDatepicker(){this._rangePicker&&this._rangePicker.open()}_shouldHideSeparator(){return(!this._formField||this._formField.getLabelId()&&!this._formField._shouldLabelFloat())&&this.empty}_getAriaLabelledby(){let e=this._formField;return e&&e._hasFloatingLabel()?e._labelId:null}_getStartDateAccessibleName(){return this._startInput._getAccessibleName()}_getEndDateAccessibleName(){return this._endInput._getAccessibleName()}_updateFocus(e){this.focused=e!==null,this.stateChanges.next()}_revalidate(){this._startInput&&this._startInput._validatorOnChange(),this._endInput&&this._endInput._validatorOnChange()}_registerModel(e){this._startInput&&this._startInput._registerModel(e),this._endInput&&this._endInput._registerModel(e)}_isTargetRequired(e){return e?.ngControl?.control?.hasValidator(Ai.required)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-date-range-input"]],hostAttrs:["role","group",1,"mat-date-range-input"],hostVars:8,hostBindings:function(i,r){i&2&&(ee("id",r.id)("aria-labelledby",r._getAriaLabelledby())("aria-describedby",r._ariaDescribedBy)("data-mat-calendar",r.rangePicker?r.rangePicker.id:null),z("mat-date-range-input-hide-placeholders",r._shouldHidePlaceholders())("mat-date-range-input-required",r.required))},inputs:{rangePicker:"rangePicker",required:[2,"required","required",j],dateFilter:"dateFilter",min:"min",max:"max",disabled:[2,"disabled","disabled",j],separator:"separator",comparisonStart:"comparisonStart",comparisonEnd:"comparisonEnd"},exportAs:["matDateRangeInput"],features:[Re([{provide:za,useExisting:t}]),Pe],ngContentSelectors:tz,decls:11,vars:5,consts:[["cdkMonitorSubtreeFocus","",1,"mat-date-range-input-container",3,"cdkFocusChange"],[1,"mat-date-range-input-wrapper"],["aria-hidden","true",1,"mat-date-range-input-mirror"],[1,"mat-date-range-input-separator"],[1,"mat-date-range-input-wrapper","mat-date-range-input-end-wrapper"]],template:function(i,r){i&1&&(ke(ez),m(0,"div",0),I("cdkFocusChange",function(a){return r._updateFocus(a)}),m(1,"div",1),se(2),m(3,"span",2),v(4),h()(),m(5,"span",3),v(6),h(),m(7,"div",4),se(8,1),m(9,"span",2),v(10),h()()()),i&2&&(g(4),G(r._getInputMirrorValue("start")),g(),z("mat-date-range-input-separator-hidden",r._shouldHideSeparator()),g(),G(r.separator),g(4),G(r._getInputMirrorValue("end")))},dependencies:[Wh],styles:[`.mat-date-range-input {
  display: block;
  width: 100%;
}

.mat-date-range-input-container {
  display: flex;
  align-items: center;
}

.mat-date-range-input-separator {
  transition: opacity 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  margin: 0 4px;
  color: var(--%NS%mat-datepicker-range-input-separator-color, var(--%NS%mat-sys-on-surface));
}
.mat-form-field-disabled .mat-date-range-input-separator {
  color: var(--%NS%mat-datepicker-range-input-disabled-state-separator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
._mat-animation-noopable .mat-date-range-input-separator {
  transition: none;
}

.mat-date-range-input-separator-hidden {
  -webkit-user-select: none;
  user-select: none;
  opacity: 0;
  transition: none;
}

.mat-date-range-input-wrapper {
  position: relative;
  overflow: hidden;
  max-width: calc(50% - 4px);
}

.mat-date-range-input-end-wrapper {
  flex-grow: 1;
}

.mat-date-range-input-inner {
  position: absolute;
  top: 0;
  left: 0;
  font: inherit;
  background: transparent;
  color: currentColor;
  border: none;
  outline: none;
  padding: 0;
  margin: 0;
  vertical-align: bottom;
  text-align: inherit;
  -webkit-appearance: none;
  width: 100%;
  height: 100%;
}
.mat-date-range-input-inner:-moz-ui-invalid {
  box-shadow: none;
}
.mat-date-range-input-inner::placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-date-range-input-inner::-moz-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-date-range-input-inner::-webkit-input-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-date-range-input-inner:-ms-input-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
}
.mat-date-range-input-inner[disabled] {
  color: var(--%NS%mat-datepicker-range-input-disabled-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-hide-placeholder .mat-date-range-input-inner::placeholder, .mat-date-range-input-hide-placeholders .mat-date-range-input-inner::placeholder {
  -webkit-user-select: none;
  user-select: none;
  color: transparent !important;
  -webkit-text-fill-color: transparent;
  transition: none;
}
@media (forced-colors: active) {
  .mat-form-field-hide-placeholder .mat-date-range-input-inner::placeholder, .mat-date-range-input-hide-placeholders .mat-date-range-input-inner::placeholder {
    opacity: 0;
  }
}
.mat-form-field-hide-placeholder .mat-date-range-input-inner::-moz-placeholder, .mat-date-range-input-hide-placeholders .mat-date-range-input-inner::-moz-placeholder {
  -webkit-user-select: none;
  user-select: none;
  color: transparent !important;
  -webkit-text-fill-color: transparent;
  transition: none;
}
@media (forced-colors: active) {
  .mat-form-field-hide-placeholder .mat-date-range-input-inner::-moz-placeholder, .mat-date-range-input-hide-placeholders .mat-date-range-input-inner::-moz-placeholder {
    opacity: 0;
  }
}
.mat-form-field-hide-placeholder .mat-date-range-input-inner::-webkit-input-placeholder, .mat-date-range-input-hide-placeholders .mat-date-range-input-inner::-webkit-input-placeholder {
  -webkit-user-select: none;
  user-select: none;
  color: transparent !important;
  -webkit-text-fill-color: transparent;
  transition: none;
}
@media (forced-colors: active) {
  .mat-form-field-hide-placeholder .mat-date-range-input-inner::-webkit-input-placeholder, .mat-date-range-input-hide-placeholders .mat-date-range-input-inner::-webkit-input-placeholder {
    opacity: 0;
  }
}
.mat-form-field-hide-placeholder .mat-date-range-input-inner:-ms-input-placeholder, .mat-date-range-input-hide-placeholders .mat-date-range-input-inner:-ms-input-placeholder {
  -webkit-user-select: none;
  user-select: none;
  color: transparent !important;
  -webkit-text-fill-color: transparent;
  transition: none;
}
@media (forced-colors: active) {
  .mat-form-field-hide-placeholder .mat-date-range-input-inner:-ms-input-placeholder, .mat-date-range-input-hide-placeholders .mat-date-range-input-inner:-ms-input-placeholder {
    opacity: 0;
  }
}
._mat-animation-noopable .mat-date-range-input-inner::placeholder {
  transition: none;
}
._mat-animation-noopable .mat-date-range-input-inner::-moz-placeholder {
  transition: none;
}
._mat-animation-noopable .mat-date-range-input-inner::-webkit-input-placeholder {
  transition: none;
}
._mat-animation-noopable .mat-date-range-input-inner:-ms-input-placeholder {
  transition: none;
}

.mat-date-range-input-mirror {
  -webkit-user-select: none;
  user-select: none;
  visibility: hidden;
  white-space: nowrap;
  display: inline-block;
  min-width: 2px;
}

.mat-mdc-form-field-type-mat-date-range-input .mat-mdc-form-field-infix {
  width: 200px;
}
`],encapsulation:2})}return t})();function mz(t){return Ub(t,!0)}function FN(t){return t.nodeType===Node.ELEMENT_NODE}function pz(t){return t.nodeName==="INPUT"}function gz(t){return t.nodeName==="TEXTAREA"}function Ub(t,n){if(FN(t)&&n){let i=(t.getAttribute?.("aria-labelledby")?.split(/\s+/g)||[]).reduce((r,o)=>{let a=document.getElementById(o);return a&&r.push(a),r},[]);if(i.length)return i.map(r=>Ub(r,!1)).join(" ")}if(FN(t)){let e=t.getAttribute("aria-label")?.trim();if(e)return e}if(pz(t)||gz(t)){if(t.labels?.length)return Array.from(t.labels).map(r=>Ub(r,!1)).join(" ");let e=t.getAttribute("placeholder")?.trim();if(e)return e;let i=t.getAttribute("title")?.trim();if(i)return i}return(t.textContent||"").replace(/\s+/g," ").trim()}var $N=(()=>{class t extends fz{_rangeInput=u(zb);_elementRef=u(U);_defaultErrorStateMatcher=u(Mo);_injector=u(te);_rawValue=S("");_parentForm=u(Ri,{optional:!0});_parentFormGroup=u(Ha,{optional:!0});ngControl;_dir=u(Tt,{optional:!0});_errorStateTracker;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}constructor(){super(),this._errorStateTracker=new Io(this._defaultErrorStateMatcher,null,this._parentFormGroup,this._parentForm,this.stateChanges)}ngOnInit(){let e=this._injector.get(hi,null,{optional:!0,self:!0});this._errorStateTracker.formField=this._injector.get(fc,null,{optional:!0,self:!0}),e&&(this.ngControl=e,this._errorStateTracker.ngControl=e)}ngAfterContentInit(){this._register()}ngDoCheck(){this.ngControl&&this.updateErrorState(),this._rawValue.set(this._elementRef.nativeElement.value)}isEmpty(){return this._rawValue().length===0}_getPlaceholder(){return this._elementRef.nativeElement.placeholder}focus(){this._elementRef.nativeElement.focus()}getMirrorValue(){let e=this._rawValue();return e.length>0?e:this._getPlaceholder()}updateErrorState(){this._errorStateTracker.updateErrorState()}_onInput(e){super._onInput(e),this._rangeInput._handleChildValueChange()}_openPopup(){this._rangeInput._openDatepicker()}_getMinDate(){return this._rangeInput.min}_getMaxDate(){return this._rangeInput.max}_getDateFilter(){return this._rangeInput.dateFilter}_parentDisabled(){return this._rangeInput._groupDisabled}_shouldHandleChangeEvent({source:e}){return e!==this._rangeInput._startInput&&e!==this._rangeInput._endInput}_assignValueProgrammatically(e,i){super._assignValueProgrammatically(e,i),(this===this._rangeInput._startInput?this._rangeInput._endInput:this._rangeInput._startInput)?._validatorOnChange(),this._rawValue.set(this._elementRef.nativeElement.value)}_formatValue(e){super._formatValue(e),this._rangeInput._handleChildValueChange()}_getAccessibleName(){return mz(this._elementRef.nativeElement)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,inputs:{errorStateMatcher:"errorStateMatcher"},features:[Be]})}return t})(),GN=(()=>{class t extends $N{_startValidator=e=>{let i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e.value)),r=this._model?this._model.selection.end:null;return!i||!r||this._dateAdapter.compareDate(i,r)<=0?null:{matStartDateInvalid:{end:r,actual:i}}};_validator=Ai.compose([...super._getValidators(),this._startValidator]);_register(){this._rangeInput._startInput=this}_getValueFromModel(e){return e.start}_shouldHandleChangeEvent(e){return super._shouldHandleChangeEvent(e)?e.oldValue?.start?!e.selection.start||!!this._dateAdapter.compareDate(e.oldValue.start,e.selection.start):!!e.selection.start:!1}_assignValueToModel(e){if(this._model){let i=new jt(e,this._model.selection.end);this._model.updateSelection(i,this),this._rangeInput._handleChildValueChange()}}_onKeydown(e){let i=this._rangeInput._endInput,r=this._elementRef.nativeElement,o=this._dir?.value!=="rtl";(e.keyCode===39&&o||e.keyCode===37&&!o)&&r.selectionStart===r.value.length&&r.selectionEnd===r.value.length?(e.preventDefault(),i._elementRef.nativeElement.setSelectionRange(0,0),i.focus()):super._onKeydown(e)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=en(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["input","matStartDate",""]],hostAttrs:["type","text",1,"mat-start-date","mat-date-range-input-inner"],hostVars:5,hostBindings:function(i,r){i&1&&I("input",function(a){return r._onInput(a)})("change",function(){return r._onChange()})("keydown",function(a){return r._onKeydown(a)})("blur",function(){return r._onBlur()}),i&2&&(pt("disabled",r.disabled),ee("aria-haspopup",r._rangeInput.rangePicker?"dialog":null)("aria-owns",r._rangeInput._ariaOwns()||null)("min",r._getMinDate()?r._dateAdapter.toIso8601(r._getMinDate()):null)("max",r._getMaxDate()?r._dateAdapter.toIso8601(r._getMaxDate()):null))},outputs:{dateChange:"dateChange",dateInput:"dateInput"},features:[Re([{provide:xo,useExisting:t,multi:!0},{provide:Lr,useExisting:t,multi:!0}]),Be]})}return t})(),WN=(()=>{class t extends $N{_endValidator=e=>{let i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e.value)),r=this._model?this._model.selection.start:null;return!i||!r||this._dateAdapter.compareDate(i,r)>=0?null:{matEndDateInvalid:{start:r,actual:i}}};_register(){this._rangeInput._endInput=this}_validator=Ai.compose([...super._getValidators(),this._endValidator]);_getValueFromModel(e){return e.end}_shouldHandleChangeEvent(e){return super._shouldHandleChangeEvent(e)?e.oldValue?.end?!e.selection.end||!!this._dateAdapter.compareDate(e.oldValue.end,e.selection.end):!!e.selection.end:!1}_assignValueToModel(e){if(this._model){let i=new jt(this._model.selection.start,e);this._model.updateSelection(i,this)}}_moveCaretToEndOfStartInput(){let e=this._rangeInput._startInput._elementRef.nativeElement,i=e.value;i.length>0&&e.setSelectionRange(i.length,i.length),e.focus()}_onKeydown(e){let i=this._elementRef.nativeElement,r=this._dir?.value!=="rtl";e.keyCode===8&&!i.value?this._moveCaretToEndOfStartInput():(e.keyCode===37&&r||e.keyCode===39&&!r)&&i.selectionStart===0&&i.selectionEnd===0?(e.preventDefault(),this._moveCaretToEndOfStartInput()):super._onKeydown(e)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=en(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["input","matEndDate",""]],hostAttrs:["type","text",1,"mat-end-date","mat-date-range-input-inner"],hostVars:5,hostBindings:function(i,r){i&1&&I("input",function(a){return r._onInput(a)})("change",function(){return r._onChange()})("keydown",function(a){return r._onKeydown(a)})("blur",function(){return r._onBlur()}),i&2&&(pt("disabled",r.disabled),ee("aria-haspopup",r._rangeInput.rangePicker?"dialog":null)("aria-owns",r._rangeInput._ariaOwns()||null)("min",r._getMinDate()?r._dateAdapter.toIso8601(r._getMinDate()):null)("max",r._getMaxDate()?r._dateAdapter.toIso8601(r._getMaxDate()):null))},outputs:{dateChange:"dateChange",dateInput:"dateInput"},features:[Re([{provide:xo,useExisting:t,multi:!0},{provide:Lr,useExisting:t,multi:!0}]),Be]})}return t})(),qN=(()=>{class t extends PN{_forwardContentValues(e){super._forwardContentValues(e);let i=this.datepickerInput;i&&(e.comparisonStart=i.comparisonStart,e.comparisonEnd=i.comparisonEnd,e.startDateAccessibleName=i._getStartDateAccessibleName(),e.endDateAccessibleName=i._getEndDateAccessibleName())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=en(t)))(r||t)}})();static \u0275cmp=F({type:t,selectors:[["mat-date-range-picker"]],exportAs:["matDateRangePicker"],features:[Re([sz,{provide:Km,useFactory:()=>u(Km,{optional:!0,skipSelf:!0})||new cz(u(Dt))},{provide:PN,useExisting:t}]),Be],decls:0,vars:0,template:function(i,r){},encapsulation:2})}return t})();var YN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({providers:[Cc],imports:[Vt,No,pd,Wd,HN,Hb,UN,Ne,jr]})}return t})();var vz=/^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|(?:(?:\+|-)\d{2}:\d{2}))?)?$/,yz=/^(\d?\d)[:.](\d?\d)(?:[:.](\d?\d))?\s*(AM|PM)?$/i;function $b(t,n){let e=Array(t);for(let i=0;i<t;i++)e[i]=n(i);return e}var bz=(()=>{class t extends Dt{_matDateLocale=u(Ab,{optional:!0});constructor(){super();let e=u(Ab,{optional:!0});e!==void 0&&(this._matDateLocale=e),super.setLocale(this._matDateLocale)}getYear(e){return e.getFullYear()}getMonth(e){return e.getMonth()}getDate(e){return e.getDate()}getDayOfWeek(e){return e.getDay()}getMonthNames(e){let i=new Intl.DateTimeFormat(this.locale,{month:e,timeZone:"utc"});return $b(12,r=>this._format(i,new Date(2017,r,1)))}getDateNames(){let e=new Intl.DateTimeFormat(this.locale,{day:"numeric",timeZone:"utc"});return $b(31,i=>this._format(e,new Date(2017,0,i+1)))}getDayOfWeekNames(e){let i=new Intl.DateTimeFormat(this.locale,{weekday:e,timeZone:"utc"});return $b(7,r=>this._format(i,new Date(2017,0,r+1)))}getYearName(e){let i=new Intl.DateTimeFormat(this.locale,{year:"numeric",timeZone:"utc"});return this._format(i,e)}getFirstDayOfWeek(){if(typeof Intl<"u"&&Intl.Locale){let e=new Intl.Locale(this.locale),i=(e.getWeekInfo?.()||e.weekInfo)?.firstDay??0;return i===7?0:i}return 0}getNumDaysInMonth(e){return this.getDate(this._createDateWithOverflow(this.getYear(e),this.getMonth(e)+1,0))}clone(e){return new Date(e.getTime())}createDate(e,i,r){let o=this._createDateWithOverflow(e,i,r);return o.getMonth()!=i,o}today(){return new Date}parse(e,i){return typeof e=="number"?new Date(e):e?new Date(Date.parse(e)):null}format(e,i){if(!this.isValid(e))throw Error("NativeDateAdapter: Cannot format invalid date.");let r=new Intl.DateTimeFormat(this.locale,re(C({},i),{timeZone:"utc"}));return this._format(r,e)}addCalendarYears(e,i){return this.addCalendarMonths(e,i*12)}addCalendarMonths(e,i){let r=this._createDateWithOverflow(this.getYear(e),this.getMonth(e)+i,this.getDate(e));return this.getMonth(r)!=((this.getMonth(e)+i)%12+12)%12&&(r=this._createDateWithOverflow(this.getYear(r),this.getMonth(r),0)),r}addCalendarDays(e,i){return this._createDateWithOverflow(this.getYear(e),this.getMonth(e),this.getDate(e)+i)}toIso8601(e){return[e.getUTCFullYear(),this._2digit(e.getUTCMonth()+1),this._2digit(e.getUTCDate())].join("-")}deserialize(e){if(typeof e=="string"){if(!e)return null;if(vz.test(e)){let i=new Date(e);if(this.isValid(i))return i}}return super.deserialize(e)}isDateInstance(e){return e instanceof Date}isValid(e){return!isNaN(e.getTime())}invalid(){return new Date(NaN)}setTime(e,i,r,o){let a=this.clone(e);return a.setHours(i,r,o,0),a}getHours(e){return e.getHours()}getMinutes(e){return e.getMinutes()}getSeconds(e){return e.getSeconds()}parseTime(e,i){if(typeof e!="string")return e instanceof Date?new Date(e.getTime()):null;let r=e.trim();if(r.length===0)return null;let o=this._parseTimeString(r);if(o===null){let a=r.replace(/[^0-9:(AM|PM)]/gi,"").trim();a.length>0&&(o=this._parseTimeString(a))}return o||this.invalid()}addSeconds(e,i){return new Date(e.getTime()+i*1e3)}_createDateWithOverflow(e,i,r){let o=new Date;return o.setFullYear(e,i,r),o.setHours(0,0,0,0),o}_2digit(e){return("00"+e).slice(-2)}_format(e,i){let r=new Date;return r.setUTCFullYear(i.getFullYear(),i.getMonth(),i.getDate()),r.setUTCHours(i.getHours(),i.getMinutes(),i.getSeconds(),i.getMilliseconds()),e.format(r)}_parseTimeString(e){let i=e.toUpperCase().match(yz);if(i){let r=parseInt(i[1]),o=parseInt(i[2]),a=i[3]==null?void 0:parseInt(i[3]),s=i[4];if(r===12?r=s==="AM"?0:r:s==="PM"&&(r+=12),Gb(r,0,23)&&Gb(o,0,59)&&(a==null||Gb(a,0,59)))return this.setTime(this.today(),r,o,a||0)}return null}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac,autoProvided:!1})}return t})();function Gb(t,n,e){return!isNaN(t)&&t>=n&&t<=e}var Cz={parse:{dateInput:null,timeInput:null},display:{dateInput:{year:"numeric",month:"numeric",day:"numeric"},timeInput:{hour:"numeric",minute:"numeric"},monthYearLabel:{year:"numeric",month:"short"},dateA11yLabel:{year:"numeric",month:"long",day:"numeric"},monthYearA11yLabel:{year:"numeric",month:"long"},timeOptionLabel:{hour:"numeric",minute:"numeric"}}};function KN(t=Cz){return[{provide:Dt,useClass:bz},{provide:ko,useValue:t}]}var Wb=[{reviewId:"seed-1",toyId:1,authorName:"Mila",authorType:"dete",rating:4,comment:"Igram se ovim svaki dan, super je!",date:"2024-01-01"},{reviewId:"seed-2",toyId:1,authorName:"Nemanja R.",authorType:"roditelj",rating:4,comment:"Materijal je bezbedan, brzo sti\u017Ee i dobro je upakovano.",date:"2024-02-06"},{reviewId:"seed-3",toyId:1,authorName:"Ana",authorType:"dete",rating:4,comment:"Malo je te\u0161ko za sastavljanje ali je zabavno.",date:"2024-03-11"},{reviewId:"seed-4",toyId:2,authorName:"Jelena S.",authorType:"roditelj",rating:4,comment:"Materijal je bezbedan, brzo sti\u017Ee i dobro je upakovano.",date:"2024-02-04"},{reviewId:"seed-5",toyId:2,authorName:"Teodora",authorType:"dete",rating:4,comment:"Malo je te\u0161ko za sastavljanje ali je zabavno.",date:"2024-03-09"},{reviewId:"seed-6",toyId:3,authorName:"Sofija",authorType:"dete",rating:4,comment:"Malo je te\u0161ko za sastavljanje ali je zabavno.",date:"2024-03-07"},{reviewId:"seed-7",toyId:3,authorName:"Bojan \u0110.",authorType:"roditelj",rating:3,comment:"Malo je skuplje nego \u0161to sam o\u010Dekivao/la, ali vredi.",date:"2024-04-12"},{reviewId:"seed-8",toyId:4,authorName:"Ivana K.",authorType:"roditelj",rating:3,comment:"Malo je skuplje nego \u0161to sam o\u010Dekivao/la, ali vredi.",date:"2024-04-10"},{reviewId:"seed-9",toyId:4,authorName:"Iva",authorType:"dete",rating:4,comment:"Volim da se igram sa bratom ovim.",date:"2024-05-15"},{reviewId:"seed-10",toyId:4,authorName:"Tijana M.",authorType:"roditelj",rating:5,comment:"Odli\u010Dna igra\u010Dka, kupujem i za ro\u0111endan drugarici.",date:"2024-06-20"},{reviewId:"seed-11",toyId:5,authorName:"Mia",authorType:"dete",rating:4,comment:"Volim da se igram sa bratom ovim.",date:"2024-05-13"},{reviewId:"seed-12",toyId:5,authorName:"Aleksandar N.",authorType:"roditelj",rating:5,comment:"Odli\u010Dna igra\u010Dka, kupujem i za ro\u0111endan drugarici.",date:"2024-06-18"},{reviewId:"seed-13",toyId:6,authorName:"Vesna J.",authorType:"roditelj",rating:5,comment:"Odli\u010Dna igra\u010Dka, kupujem i za ro\u0111endan drugarici.",date:"2024-06-16"},{reviewId:"seed-14",toyId:6,authorName:"Sara",authorType:"dete",rating:4,comment:"Mogu satima da se igram ovim.",date:"2024-07-21"},{reviewId:"seed-15",toyId:7,authorName:"Ema",authorType:"dete",rating:4,comment:"Mogu satima da se igram ovim.",date:"2024-07-19"},{reviewId:"seed-16",toyId:7,authorName:"Igor S.",authorType:"roditelj",rating:4,comment:"Dete je odu\u0161evljeno, kvalitet je odli\u010Dan za tu cenu.",date:"2024-08-24"},{reviewId:"seed-17",toyId:7,authorName:"Mila",authorType:"dete",rating:4,comment:"Ba\u0161 mi se dopada, boj\u0435 su prelepe.",date:"2024-09-02"},{reviewId:"seed-18",toyId:8,authorName:"Sne\u017Eana P.",authorType:"roditelj",rating:4,comment:"Dete je odu\u0161evljeno, kvalitet je odli\u010Dan za tu cenu.",date:"2024-08-22"},{reviewId:"seed-19",toyId:8,authorName:"Ana",authorType:"dete",rating:4,comment:"Ba\u0161 mi se dopada, boj\u0435 su prelepe.",date:"2024-09-27"},{reviewId:"seed-20",toyId:9,authorName:"Teodora",authorType:"dete",rating:4,comment:"Ba\u0161 mi se dopada, boj\u0435 su prelepe.",date:"2024-09-25"},{reviewId:"seed-21",toyId:9,authorName:"Predrag K.",authorType:"roditelj",rating:3,comment:"Preporu\u010Dujem, razvija ma\u0161tu i motoriku kod deteta.",date:"2024-10-03"},{reviewId:"seed-22",toyId:10,authorName:"Milena R.",authorType:"roditelj",rating:3,comment:"Preporu\u010Dujem, razvija ma\u0161tu i motoriku kod deteta.",date:"2024-10-01"},{reviewId:"seed-23",toyId:10,authorName:"Jovana",authorType:"dete",rating:4,comment:"Ovo mi je najdra\u017Ea igra\u010Dka do sada!",date:"2024-11-06"},{reviewId:"seed-24",toyId:10,authorName:"Marija P.",authorType:"roditelj",rating:5,comment:"Dete se brzo zasitilo, o\u010Dekivali smo vi\u0161e.",date:"2024-12-11"},{reviewId:"seed-25",toyId:11,authorName:"Iva",authorType:"dete",rating:4,comment:"Ovo mi je najdra\u017Ea igra\u010Dka do sada!",date:"2024-11-04"},{reviewId:"seed-26",toyId:11,authorName:"Nemanja R.",authorType:"roditelj",rating:5,comment:"Dete se brzo zasitilo, o\u010Dekivali smo vi\u0161e.",date:"2024-12-09"},{reviewId:"seed-27",toyId:12,authorName:"Jelena S.",authorType:"roditelj",rating:5,comment:"Dete se brzo zasitilo, o\u010Dekivali smo vi\u0161e.",date:"2024-12-07"},{reviewId:"seed-28",toyId:12,authorName:"Lena",authorType:"dete",rating:4,comment:"Super je za no\u0161enje na spavanje.",date:"2024-01-12"},{reviewId:"seed-29",toyId:13,authorName:"Sara",authorType:"dete",rating:4,comment:"Super je za no\u0161enje na spavanje.",date:"2024-01-10"},{reviewId:"seed-30",toyId:13,authorName:"Bojan \u0110.",authorType:"roditelj",rating:4,comment:"Traje dugo i lako se odr\u017Eava \u010Distim.",date:"2024-02-15"},{reviewId:"seed-31",toyId:13,authorName:"Ema",authorType:"dete",rating:4,comment:"Igram se ovim svaki dan, super je!",date:"2024-03-20"},{reviewId:"seed-32",toyId:14,authorName:"Ivana K.",authorType:"roditelj",rating:4,comment:"Traje dugo i lako se odr\u017Eava \u010Distim.",date:"2024-02-13"},{reviewId:"seed-33",toyId:14,authorName:"Mila",authorType:"dete",rating:4,comment:"Igram se ovim svaki dan, super je!",date:"2024-03-18"},{reviewId:"seed-34",toyId:15,authorName:"Ana",authorType:"dete",rating:4,comment:"Igram se ovim svaki dan, super je!",date:"2024-03-16"},{reviewId:"seed-35",toyId:15,authorName:"Aleksandar N.",authorType:"roditelj",rating:3,comment:"Materijal je bezbedan, brzo sti\u017Ee i dobro je upakovano.",date:"2024-04-21"},{reviewId:"seed-36",toyId:16,authorName:"Vesna J.",authorType:"roditelj",rating:3,comment:"Materijal je bezbedan, brzo sti\u017Ee i dobro je upakovano.",date:"2024-04-19"},{reviewId:"seed-37",toyId:16,authorName:"Sofija",authorType:"dete",rating:4,comment:"Malo je te\u0161ko za sastavljanje ali je zabavno.",date:"2024-05-24"},{reviewId:"seed-38",toyId:16,authorName:"Katarina \u0110.",authorType:"roditelj",rating:5,comment:"Malo je skuplje nego \u0161to sam o\u010Dekivao/la, ali vredi.",date:"2024-06-02"},{reviewId:"seed-39",toyId:17,authorName:"Jovana",authorType:"dete",rating:4,comment:"Malo je te\u0161ko za sastavljanje ali je zabavno.",date:"2024-05-22"},{reviewId:"seed-40",toyId:17,authorName:"Igor S.",authorType:"roditelj",rating:5,comment:"Malo je skuplje nego \u0161to sam o\u010Dekivao/la, ali vredi.",date:"2024-06-27"},{reviewId:"seed-41",toyId:18,authorName:"Sne\u017Eana P.",authorType:"roditelj",rating:5,comment:"Malo je skuplje nego \u0161to sam o\u010Dekivao/la, ali vredi.",date:"2024-06-25"},{reviewId:"seed-42",toyId:18,authorName:"Mia",authorType:"dete",rating:4,comment:"Volim da se igram sa bratom ovim.",date:"2024-07-03"},{reviewId:"seed-43",toyId:19,authorName:"Lena",authorType:"dete",rating:4,comment:"Volim da se igram sa bratom ovim.",date:"2024-07-01"},{reviewId:"seed-44",toyId:19,authorName:"Predrag K.",authorType:"roditelj",rating:4,comment:"Odli\u010Dna igra\u010Dka, kupujem i za ro\u0111endan drugarici.",date:"2024-08-06"},{reviewId:"seed-45",toyId:19,authorName:"Sara",authorType:"dete",rating:4,comment:"Mogu satima da se igram ovim.",date:"2024-09-11"},{reviewId:"seed-46",toyId:20,authorName:"Milena R.",authorType:"roditelj",rating:4,comment:"Odli\u010Dna igra\u010Dka, kupujem i za ro\u0111endan drugarici.",date:"2024-08-04"},{reviewId:"seed-47",toyId:20,authorName:"Ema",authorType:"dete",rating:4,comment:"Mogu satima da se igram ovim.",date:"2024-09-09"},{reviewId:"seed-48",toyId:21,authorName:"Mila",authorType:"dete",rating:4,comment:"Mogu satima da se igram ovim.",date:"2024-09-07"},{reviewId:"seed-49",toyId:21,authorName:"Nemanja R.",authorType:"roditelj",rating:3,comment:"Dete je odu\u0161evljeno, kvalitet je odli\u010Dan za tu cenu.",date:"2024-10-12"},{reviewId:"seed-50",toyId:22,authorName:"Jelena S.",authorType:"roditelj",rating:3,comment:"Dete je odu\u0161evljeno, kvalitet je odli\u010Dan za tu cenu.",date:"2024-10-10"},{reviewId:"seed-51",toyId:22,authorName:"Teodora",authorType:"dete",rating:4,comment:"Ba\u0161 mi se dopada, boj\u0435 su prelepe.",date:"2024-11-15"},{reviewId:"seed-52",toyId:22,authorName:"Milica V.",authorType:"roditelj",rating:5,comment:"Preporu\u010Dujem, razvija ma\u0161tu i motoriku kod deteta.",date:"2024-12-20"},{reviewId:"seed-53",toyId:23,authorName:"Sofija",authorType:"dete",rating:4,comment:"Ba\u0161 mi se dopada, boj\u0435 su prelepe.",date:"2024-11-13"},{reviewId:"seed-54",toyId:23,authorName:"Bojan \u0110.",authorType:"roditelj",rating:5,comment:"Preporu\u010Dujem, razvija ma\u0161tu i motoriku kod deteta.",date:"2024-12-18"},{reviewId:"seed-55",toyId:24,authorName:"Ivana K.",authorType:"roditelj",rating:5,comment:"Preporu\u010Dujem, razvija ma\u0161tu i motoriku kod deteta.",date:"2024-12-16"},{reviewId:"seed-56",toyId:24,authorName:"Iva",authorType:"dete",rating:4,comment:"Ovo mi je najdra\u017Ea igra\u010Dka do sada!",date:"2024-01-21"},{reviewId:"seed-57",toyId:25,authorName:"Mia",authorType:"dete",rating:4,comment:"Ovo mi je najdra\u017Ea igra\u010Dka do sada!",date:"2024-01-19"},{reviewId:"seed-58",toyId:25,authorName:"Aleksandar N.",authorType:"roditelj",rating:4,comment:"Dete se brzo zasitilo, o\u010Dekivali smo vi\u0161e.",date:"2024-02-24"},{reviewId:"seed-59",toyId:25,authorName:"Lena",authorType:"dete",rating:4,comment:"Super je za no\u0161enje na spavanje.",date:"2024-03-02"},{reviewId:"seed-60",toyId:26,authorName:"Vesna J.",authorType:"roditelj",rating:4,comment:"Dete se brzo zasitilo, o\u010Dekivali smo vi\u0161e.",date:"2024-02-22"},{reviewId:"seed-61",toyId:26,authorName:"Sara",authorType:"dete",rating:4,comment:"Super je za no\u0161enje na spavanje.",date:"2024-03-27"},{reviewId:"seed-62",toyId:27,authorName:"Ema",authorType:"dete",rating:4,comment:"Super je za no\u0161enje na spavanje.",date:"2024-03-25"},{reviewId:"seed-63",toyId:27,authorName:"Igor S.",authorType:"roditelj",rating:3,comment:"Traje dugo i lako se odr\u017Eava \u010Distim.",date:"2024-04-03"},{reviewId:"seed-64",toyId:28,authorName:"Sne\u017Eana P.",authorType:"roditelj",rating:3,comment:"Traje dugo i lako se odr\u017Eava \u010Distim.",date:"2024-04-01"},{reviewId:"seed-65",toyId:28,authorName:"Ana",authorType:"dete",rating:4,comment:"Igram se ovim svaki dan, super je!",date:"2024-05-06"},{reviewId:"seed-66",toyId:28,authorName:"Ana M.",authorType:"roditelj",rating:5,comment:"Materijal je bezbedan, brzo sti\u017Ee i dobro je upakovano.",date:"2024-06-11"},{reviewId:"seed-67",toyId:29,authorName:"Teodora",authorType:"dete",rating:4,comment:"Igram se ovim svaki dan, super je!",date:"2024-05-04"},{reviewId:"seed-68",toyId:29,authorName:"Predrag K.",authorType:"roditelj",rating:5,comment:"Materijal je bezbedan, brzo sti\u017Ee i dobro je upakovano.",date:"2024-06-09"},{reviewId:"seed-69",toyId:30,authorName:"Milena R.",authorType:"roditelj",rating:5,comment:"Materijal je bezbedan, brzo sti\u017Ee i dobro je upakovano.",date:"2024-06-07"},{reviewId:"seed-70",toyId:30,authorName:"Jovana",authorType:"dete",rating:4,comment:"Malo je te\u0161ko za sastavljanje ali je zabavno.",date:"2024-07-12"}];var QN="toybox_users",qb="toybox_current_user",XN="toybox_reservations";async function Yb(t){let e=new TextEncoder().encode(t),i=await crypto.subtle.digest("SHA-256",e);return Array.from(new Uint8Array(i)).map(o=>o.toString(16).padStart(2,"0")).join("")}function ZN(){return crypto.randomUUID()}function Zm(){let t=localStorage.getItem(QN);return t?JSON.parse(t):[]}function Kb(t){localStorage.setItem(QN,JSON.stringify(t))}function wz(){let t=localStorage.getItem(XN);return t?JSON.parse(t).map(e=>re(C({},e),{quantity:e.quantity??1})):[]}function Qm(t){localStorage.setItem(XN,JSON.stringify(t))}var Ke=class t{static currentUser=S(t.loadCurrentUserFromStorage());static reservations=S(wz());static activeCartCount=ct(()=>{let n=t.currentUser();return n?t.reservations().filter(e=>e.userId===n.userId&&e.status!=="otkazano").reduce((e,i)=>e+i.quantity,0):0});static loadCurrentUserFromStorage(){let n=localStorage.getItem(qb);return n?JSON.parse(n):null}static async signup(n,e,i,r,o,a,s=[]){let c=Zm();if(c.find(p=>p.email.toLowerCase()===n.toLowerCase()))throw new Error("Korisnik sa ovim email-om ve\u0107 postoji.");let d=await Yb(e),f={userId:ZN(),email:n,passwordHash:d,firstName:i,lastName:r,phone:o,address:a,favoriteToyTypes:s};return c.push(f),Kb(c),t.setCurrentUser(f),f}static async login(n,e){let r=Zm().find(a=>a.email.toLowerCase()===n.toLowerCase());if(!r)throw new Error("Pogre\u0161an email ili lozinka.");if(await Yb(e)!==r.passwordHash)throw new Error("Pogre\u0161an email ili lozinka.");return t.setCurrentUser(r),r}static logout(){localStorage.removeItem(qb),t.currentUser.set(null)}static setCurrentUser(n){localStorage.setItem(qb,JSON.stringify(n)),t.currentUser.set(n)}static getCurrentUser(){return t.currentUser()}static updateProfile(n,e){let i=Zm(),r=i.find(o=>o.userId===n);if(!r)throw new Error("Korisnik nije prona\u0111en.");if(e.email&&e.email.toLowerCase()!==r.email.toLowerCase()&&i.some(a=>a.userId!==n&&a.email.toLowerCase()===e.email.toLowerCase()))throw new Error("Korisnik sa ovim email-om ve\u0107 postoji.");return Object.assign(r,e),Kb(i),t.getCurrentUser()?.userId===n&&t.setCurrentUser(r),r}static async changePassword(n,e){let i=Zm(),r=i.find(o=>o.userId===n);if(!r)throw new Error("Korisnik nije prona\u0111en.");r.passwordHash=await Yb(e),Kb(i),t.getCurrentUser()?.userId===n&&t.setCurrentUser(r)}static createReservation(n){let e=t.getCurrentUser();if(!e)throw new Error("Morate biti prijavljeni da biste napravili rezervaciju.");let i=t.reservations(),r=i.find(s=>s.userId===e.userId&&s.toyId===n.toyId&&s.status==="rezervisano");if(r){let s=i.map(l=>l.reservationId===r.reservationId?re(C({},l),{quantity:l.quantity+1}):l);return Qm(s),t.reservations.set(s),{reservation:s.find(l=>l.reservationId===r.reservationId),merged:!0}}let o={reservationId:ZN(),userId:e.userId,toyId:n.toyId,toyName:n.name,toyDescription:n.description,toyTypeName:n.type.name,toyAgeGroupName:n.ageGroup.name,toyTargetGroup:n.targetGroup,toyProductionDate:n.productionDate,toyPrice:n.price,toyImageUrl:n.imageUrl,status:"rezervisano",reservationDate:new Date().toISOString(),quantity:1},a=[...i,o];return Qm(a),t.reservations.set(a),{reservation:o,merged:!1}}static getReservationsForUser(n){return t.reservations().filter(e=>e.userId===n)}static updateReservation(n,e){let i=t.reservations(),r=i.find(a=>a.reservationId===n);if(!r)throw new Error("Rezervacija nije prona\u0111ena.");e(r);let o=[...i];Qm(o),t.reservations.set(o)}static updateReservationStatus(n,e){t.updateReservation(n,i=>{i.status=e})}static updateReservationQuantity(n,e){if(!Number.isInteger(e)||e<1)throw new Error("Koli\u010Dina mora biti ceo broj ve\u0107i od nule.");t.updateReservation(n,i=>{if(i.status!=="rezervisano")throw new Error('Koli\u010Dina se mo\u017Ee menjati samo za rezervacije sa statusom "rezervisano".');i.quantity=e})}static markReservationArrived(n){t.updateReservation(n,e=>{if(e.status!=="rezervisano")throw new Error("Samo rezervisane igra\u010Dke mogu biti ozna\u010Dene kao pristigle.");e.status="pristiglo"})}static deleteReservation(n){let e=t.reservations().filter(i=>i.reservationId!==n);Qm(e),t.reservations.set(e)}static rateReservation(n,e){t.updateReservation(n,i=>{if(i.status!=="pristiglo")throw new Error('Ocenjivanje je mogu\u0107e samo za rezervacije sa statusom "pristiglo".');i.rating=e})}};var JN="toybox_user_reviews";function Sz(){return crypto.randomUUID()}function Zb(){let t=localStorage.getItem(JN);return t?JSON.parse(t):[]}function Dz(t){localStorage.setItem(JN,JSON.stringify(t))}var zr=class t{static getReviewsForToy(n){return[...Wb,...Zb()].filter(i=>i.toyId===n).sort((i,r)=>new Date(r.date).getTime()-new Date(i.date).getTime())}static addReview(n,e,i,r){let o=Ke.getCurrentUser();if(!o)throw new Error("Morate biti prijavljeni da biste dodali recenziju.");if(i<1||i>5)throw new Error("Ocena mora biti izme\u0111u 1 i 5.");if(!r.trim())throw new Error("Komentar ne sme biti prazan.");let a={reviewId:Sz(),toyId:n,authorName:`${o.firstName} ${o.lastName}`,authorType:e,rating:i,comment:r.trim(),date:new Date().toISOString()},s=Zb();return s.push(a),Dz(s),a}static findToyIdsByReviewText(n){let e=n.trim().toLowerCase(),i=new Set;if(!e)return i;let r=[...Wb,...Zb()];for(let o of r)o.comment.toLowerCase().includes(e)&&i.add(o.toyId);return i}static getAverageRating(n){let e=t.getReviewsForToy(n);if(e.length===0)return null;let i=e.reduce((r,o)=>r+o.rating,0);return Math.round(i/e.length*10)/10}};var Kt=class t{static searchTerm=S("");static reviewSearchTerm=S("");static selectedTypeIds=S(new Set);static selectedAgeGroupIds=S(new Set);static selectedTargetGroup=S("");static minPrice=S(null);static maxPrice=S(null);static dateFrom=S(null);static dateTo=S(null);static clear(){t.searchTerm.set(""),t.reviewSearchTerm.set(""),t.selectedTypeIds.set(new Set),t.selectedAgeGroupIds.set(new Set),t.selectedTargetGroup.set(""),t.minPrice.set(null),t.maxPrice.set(null),t.dateFrom.set(null),t.dateTo.set(null)}};var Qb={svi:"Uniseks",de\u010Dak:"De\u010Daci",devoj\u010Dica:"Devoj\u010Dice"};function Xm(t){return Qb[t]??t}var eT=()=>({updateOn:"blur"}),Ez=t=>["/igracke",t],xz=(t,n)=>n.typeId,Mz=(t,n)=>n.ageGroupId,Iz=(t,n)=>n.value,Nz=(t,n)=>n.toyId;function Tz(t,n){t&1&&(m(0,"p"),v(1,"U\u010Ditavanje..."),h())}function kz(t,n){if(t&1&&(m(0,"p",2),v(1),h()),t&2){let e=D();g(),G(e.error())}}function Az(t,n){if(t&1){let e=Ie();m(0,"mat-checkbox",25),I("change",function(){let r=q(e).$implicit,o=D(2);return Y(o.toggleType(r.typeId))}),v(1),h()}if(t&2){let e=n.$implicit,i=D(2);T("checked",i.selectedTypeIds().has(e.typeId)),g(),pe(" ",e.name," ")}}function Rz(t,n){if(t&1){let e=Ie();m(0,"mat-checkbox",25),I("change",function(){let r=q(e).$implicit,o=D(2);return Y(o.toggleAgeGroup(r.ageGroupId))}),v(1),h()}if(t&2){let e=n.$implicit,i=D(2);T("checked",i.selectedAgeGroupIds().has(e.ageGroupId)),g(),pe(" ",e.name," ")}}function Oz(t,n){if(t&1&&(m(0,"mat-option",11),v(1),h()),t&2){let e=n.$implicit;T("value",e.value),g(),G(e.label)}}function Pz(t,n){t&1&&(m(0,"p",22),v(1,"Nema igra\u010Daka koje odgovaraju izabranim filterima."),h())}function Fz(t,n){if(t&1&&(m(0,"mat-card",24),de(1,"img",26),m(2,"mat-card-header")(3,"mat-card-title"),v(4),h(),m(5,"mat-card-subtitle"),v(6),h()(),m(7,"mat-card-content")(8,"p"),v(9),h(),m(10,"p",27),v(11),h()()()),t&2){let e=n.$implicit,i=D(2);T("routerLink",Ir(8,Ez,e.permalink)),g(),T("src",i.imageBaseUrl+e.imageUrl,tr)("alt",e.name),g(3),G(e.name),g(2),ya("",e.type.name," \xB7 ",e.ageGroup.name),g(3),G(e.description),g(2),pe("",e.price," RSD")}}function Lz(t,n){if(t&1){let e=Ie();m(0,"div",3)(1,"aside",4)(2,"h2"),v(3,"Filteri"),h(),m(4,"mat-form-field",5)(5,"mat-label"),v(6,"Pretraga"),h(),m(7,"input",6),I("ngModelChange",function(r){q(e);let o=D();return Y(o.searchTerm.set(r))}),h(),Fe(),h(),m(8,"mat-form-field",5)(9,"mat-label"),v(10,"Pretraga po recenzijama"),h(),m(11,"input",7),I("ngModelChange",function(r){q(e);let o=D();return Y(o.reviewSearchTerm.set(r))}),h(),Fe(),h(),m(12,"div",8)(13,"h3"),v(14,"Tip"),h(),at(15,Az,2,2,"mat-checkbox",9,xz),h(),m(17,"div",8)(18,"h3"),v(19,"Uzrasna grupa"),h(),at(20,Rz,2,2,"mat-checkbox",9,Mz),h(),m(22,"div",8)(23,"h3"),v(24,"Ciljna grupa"),h(),m(25,"mat-form-field",5)(26,"mat-select",10),I("ngModelChange",function(r){q(e);let o=D();return Y(o.selectedTargetGroup.set(r))}),at(27,Oz,2,2,"mat-option",11,Iz),h(),Fe(),h()(),m(29,"div",8)(30,"h3"),v(31,"Cena (RSD)"),h(),m(32,"div",12)(33,"mat-form-field",13)(34,"mat-label"),v(35,"Od"),h(),m(36,"input",14),I("ngModelChange",function(r){q(e);let o=D();return Y(o.minPrice.set(r))}),h(),Fe(),h(),m(37,"mat-form-field",13)(38,"mat-label"),v(39,"Do"),h(),m(40,"input",14),I("ngModelChange",function(r){q(e);let o=D();return Y(o.maxPrice.set(r))}),h(),Fe(),h()()(),m(41,"div",8)(42,"h3"),v(43,"Datum proizvodnje"),h(),m(44,"mat-form-field",5)(45,"mat-label"),v(46,"Od - Do"),h(),m(47,"mat-date-range-input",15)(48,"input",16),I("ngModelChange",function(r){q(e);let o=D();return Y(o.dateFrom.set(r))}),h(),Fe(),m(49,"input",17),I("ngModelChange",function(r){q(e);let o=D();return Y(o.dateTo.set(r))}),h(),Fe(),h(),de(50,"mat-datepicker-toggle",18)(51,"mat-date-range-picker",null,0),h()(),m(53,"button",19),I("click",function(){q(e);let r=D();return Y(r.clearFilters())}),v(54," Obri\u0161i filtere "),h()(),m(55,"div",20)(56,"p",21),v(57),h(),R(58,Pz,2,0,"p",22),m(59,"div",23),at(60,Fz,12,10,"mat-card",24,Nz),h()()()}if(t&2){let e=ii(52),i=D();g(7),T("ngModel",i.searchTerm()),Le(),g(4),T("ngModel",i.reviewSearchTerm()),Le(),g(4),st(i.types()),g(5),st(i.ageGroups()),g(6),T("ngModel",i.selectedTargetGroup()),Le(),g(),st(i.targetGroupOptions),g(9),T("ngModel",i.minPrice())("ngModelOptions",mo(15,eT)),Le(),g(4),T("ngModel",i.maxPrice())("ngModelOptions",mo(16,eT)),Le(),g(7),T("rangePicker",e)("min",i.minProductionDate)("max",i.maxProductionDate),g(),T("ngModel",i.dateFrom()),Le(),g(),T("ngModel",i.dateTo()),Le(),g(),T("for",e),g(7),pe("",i.filteredToys().length," igra\u010Daka prona\u0111eno"),g(),O(i.filteredToys().length===0?58:-1),g(2),st(i.filteredToys())}}var Jm=class t{allToys=S([]);ageGroups=S([]);types=S([]);loading=S(!0);error=S(null);imageBaseUrl="https://toy.pequla.com";minProductionDate=new Date(2e3,0,1);maxProductionDate=new Date;targetGroupOptions=[{value:"",label:"Sve ciljne grupe"},...Object.entries(Qb).map(([n,e])=>({value:n,label:e}))];searchTerm=Kt.searchTerm;reviewSearchTerm=Kt.reviewSearchTerm;selectedTypeIds=Kt.selectedTypeIds;selectedAgeGroupIds=Kt.selectedAgeGroupIds;selectedTargetGroup=Kt.selectedTargetGroup;minPrice=Kt.minPrice;maxPrice=Kt.maxPrice;dateFrom=Kt.dateFrom;dateTo=Kt.dateTo;filteredToys=ct(()=>{let n=this.searchTerm().trim().toLowerCase(),e=this.reviewSearchTerm().trim(),i=e?zr.findToyIdsByReviewText(e):null,r=this.selectedTypeIds(),o=this.selectedAgeGroupIds(),a=this.selectedTargetGroup(),s=this.minPrice(),c=this.maxPrice(),l=this.dateFrom(),d=this.dateTo();return this.allToys().filter(f=>!(n&&!f.name.toLowerCase().includes(n)&&!f.description.toLowerCase().includes(n)||i&&!i.has(f.toyId)||r.size>0&&!r.has(f.type.typeId)||o.size>0&&!o.has(f.ageGroup.ageGroupId)||a&&f.targetGroup!==a||s!=null&&f.price<s||c!=null&&f.price>c||l&&new Date(f.productionDate).getTime()<l.getTime()||d&&new Date(f.productionDate).getTime()>d.getTime()))});ngOnInit(){this.loadToys(),this.loadFilters()}async loadToys(){this.loading.set(!0),this.error.set(null);try{let n=await pn.getToys();this.allToys.set(n.data)}catch{this.error.set("Ne mogu da u\u010Ditam igra\u010Dke. Poku\u0161ajte ponovo.")}finally{this.loading.set(!1)}}async loadFilters(){try{let[n,e]=await Promise.all([pn.getAgeGroups(),pn.getTypes()]);this.ageGroups.set(n.data),this.types.set(e.data)}catch(n){console.error("Gre\u0161ka pri u\u010Ditavanju filtera",n)}}toggleType(n){let e=new Set(this.selectedTypeIds());e.has(n)?e.delete(n):e.add(n),this.selectedTypeIds.set(e)}toggleAgeGroup(n){let e=new Set(this.selectedAgeGroupIds());e.has(n)?e.delete(n):e.add(n),this.selectedAgeGroupIds.set(e)}clearFilters(){Kt.clear()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=F({type:t,selectors:[["app-catalog"]],features:[Re([KN()])],decls:6,vars:3,consts:[["productionDatePicker",""],[1,"catalog"],[1,"error"],[1,"catalog-layout"],[1,"sidebar"],["appearance","outline",1,"full-width"],["matInput","","placeholder","Naziv ili opis...",3,"ngModelChange","ngModel"],["matInput","","placeholder","Re\u010D iz komentara recenzije...",3,"ngModelChange","ngModel"],[1,"filter-group"],[3,"checked"],[3,"ngModelChange","ngModel"],[3,"value"],[1,"range-inputs"],["appearance","outline",1,"range-field"],["matInput","","type","number",3,"ngModelChange","ngModel","ngModelOptions"],[3,"rangePicker","min","max"],["matStartDate","","placeholder","Od",3,"ngModelChange","ngModel"],["matEndDate","","placeholder","Do",3,"ngModelChange","ngModel"],["matSuffix","",3,"for"],["mat-stroked-button","",1,"clear-button",3,"click"],[1,"content"],[1,"results-count"],[1,"no-results"],[1,"toy-grid"],[1,"toy-card",3,"routerLink"],[3,"change","checked"],["mat-card-image","",3,"src","alt"],[1,"price"]],template:function(e,i){e&1&&(m(0,"div",1)(1,"h1"),v(2,"Igra\u010Dke"),h(),R(3,Tz,2,0,"p"),R(4,kz,2,1,"p",2),R(5,Lz,62,17,"div",3),h()),e&2&&(g(3),O(i.loading()?3:-1),g(),O(i.error()?4:-1),g(),O(!i.loading()&&!i.error()?5:-1))},dependencies:[Ht,xn,En,am,yM,sm,_M,gM,zI,yb,gn,gi,Wn,Vr,lr,cr,qm,Wm,To,Vt,St,YN,Hb,zb,GN,WN,qN,pi,Tn,gb,mi,Gn],styles:[".catalog[_ngcontent-%COMP%]{max-width:1400px;margin:0 auto;padding:24px}.catalog-layout[_ngcontent-%COMP%]{display:flex;gap:24px;align-items:flex-start}.sidebar[_ngcontent-%COMP%]{width:260px;flex-shrink:0;display:flex;flex-direction:column;gap:8px;position:sticky;top:88px;max-height:calc(100vh - 104px);overflow-y:auto}.sidebar[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{margin:0 0 8px;font-size:18px}.filter-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:6px;margin-bottom:8px}.filter-group[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0 0 4px;font-size:14px;font-weight:600}.full-width[_ngcontent-%COMP%]{width:100%}.range-inputs[_ngcontent-%COMP%]{display:flex;gap:8px}.range-inputs-stacked[_ngcontent-%COMP%]{flex-direction:column}.range-field[_ngcontent-%COMP%]{flex:1}.clear-button[_ngcontent-%COMP%]{margin-top:8px}.content[_ngcontent-%COMP%]{flex:1;min-width:0}.results-count[_ngcontent-%COMP%]{margin:0 0 12px;font-size:14px;color:var(--%NS%mat-sys-outline)}.no-results[_ngcontent-%COMP%]{padding:32px;text-align:center;color:var(--%NS%mat-sys-outline)}.toy-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px}.toy-card[_ngcontent-%COMP%]{display:flex;flex-direction:column;cursor:pointer}.toy-card[_ngcontent-%COMP%]   img[_ngcontent-%COMP%]{height:180px;object-fit:cover}.price[_ngcontent-%COMP%]{font-weight:600;color:var(--%NS%mat-sys-secondary)}"]})};var Vz=t=>({returnUrl:t}),jz=()=>({});function Bz(t,n){if(t&1&&(m(0,"p",8),v(1),h()),t&2){let e=D();g(),G(e.error())}}var ep=class t{constructor(n,e,i){this.router=n;this.route=e;this.location=i;this.returnUrl=this.route.snapshot.queryParamMap.get("returnUrl")}router;route;location;email=S("");password=S("");showPassword=S(!1);error=S(null);loading=S(!1);returnUrl=null;goBack(){this.location.back()}toggleShowPassword(){this.showPassword.set(!this.showPassword())}async onSubmit(){this.error.set(null),this.loading.set(!0);try{await Ke.login(this.email(),this.password()),this.router.navigateByUrl(this.returnUrl??"/")}catch(n){this.error.set(n instanceof Error?n.message:"Prijava nije uspela.")}finally{this.loading.set(!1)}}static \u0275fac=function(e){return new(e||t)(ie(qt),ie(mn),ie(ai))};static \u0275cmp=F({type:t,selectors:[["app-login"]],decls:27,vars:12,consts:[[1,"auth-page"],[1,"auth-card"],["mat-button","","type","button",1,"back-link",3,"click"],[3,"ngSubmit"],["appearance","outline",1,"full-width"],["matInput","","type","email","name","email","required","",3,"ngModelChange","ngModel"],["matInput","","name","password","required","",3,"ngModelChange","type","ngModel"],["mat-icon-button","","matSuffix","","type","button",3,"click"],[1,"error"],["mat-raised-button","","color","primary","type","submit",1,"full-width",3,"disabled"],[1,"switch-link"],["routerLink","/signup",3,"queryParams"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card",1)(2,"button",2),I("click",function(){return i.goBack()}),m(3,"mat-icon"),v(4,"arrow_back"),h(),v(5," Nazad "),h(),m(6,"h1"),v(7,"Prijava"),h(),m(8,"form",3),I("ngSubmit",function(){return i.onSubmit()}),m(9,"mat-form-field",4)(10,"mat-label"),v(11,"Email"),h(),m(12,"input",5),I("ngModelChange",function(o){return i.email.set(o)}),h(),Fe(),h(),m(13,"mat-form-field",4)(14,"mat-label"),v(15,"Lozinka"),h(),m(16,"input",6),I("ngModelChange",function(o){return i.password.set(o)}),h(),Fe(),m(17,"button",7),I("click",function(){return i.toggleShowPassword()}),m(18,"mat-icon"),v(19),h()()(),R(20,Bz,2,1,"p",8),m(21,"button",9),v(22),h()(),m(23,"p",10),v(24," Nemate nalog? "),m(25,"a",11),v(26,"Registrujte se"),h()()()()),e&2&&(g(12),T("ngModel",i.email()),Le(),g(4),T("type",i.showPassword()?"text":"password")("ngModel",i.password()),Le(),g(),ee("aria-label",i.showPassword()?"Sakrij lozinku":"Prika\u017Ei lozinku"),g(2),G(i.showPassword()?"visibility_off":"visibility"),g(),O(i.error()?20:-1),g(),T("disabled",i.loading()),g(),pe(" ",i.loading()?"Prijavljivanje...":"Prijavi se"," "),g(3),T("queryParams",i.returnUrl?Ir(9,Vz,i.returnUrl):mo(11,jz)))},dependencies:[pi,Fm,Tn,mi,Pm,dc,Gn,Ri,Ht,xn,En,gn,gi,Wn,Vr,lr,cr,Vt,St,or,In,Mn],styles:[".auth-page[_ngcontent-%COMP%]{display:flex;justify-content:center;padding:48px 24px}.auth-card[_ngcontent-%COMP%]{width:100%;max-width:400px;padding:24px}.auth-card[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0 0 24px;text-align:center}.full-width[_ngcontent-%COMP%]{width:100%;margin-bottom:8px}.error[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-error);font-size:14px;margin:0 0 12px}.switch-link[_ngcontent-%COMP%]{text-align:center;margin-top:16px;font-size:14px}"]})};var Uz=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],Hz=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function zz(t,n){t&1&&(m(0,"span",3),se(1,1),h())}function $z(t,n){t&1&&(m(0,"span",6),se(1,2),h())}var Gz=["*"];var Wz=new w("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),Xb=new w("MatChipAvatar"),tT=new w("MatChipTrailingIcon"),nT=new w("MatChipEdit"),iT=new w("MatChipRemove"),rT=new w("MatChip"),oT=(()=>{class t{_elementRef=u(U);_parentChip=u(rT);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(e){this._disabled=e}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){u(Ge).load($n),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(ee("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),z("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",j],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?-1:ir(e)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return t})(),qz=(()=>{class t extends oT{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(e){!this.disabled&&this._isPrimary&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=en(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&I("click",function(a){return r._handleClick(a)})("keydown",function(a){return r._handleKeydown(a)}),i&2&&(ee("tabindex",r._getTabindex()),z("mdc-evolution-chip__action--presentational",!1))},features:[Be]})}return t})(),tp=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-chip-avatar"],["","matChipAvatar",""]],hostAttrs:["role","img",1,"mat-mdc-chip-avatar","mdc-evolution-chip__icon","mdc-evolution-chip__icon--primary"],features:[Re([{provide:Xb,useExisting:t}])]})}return t})();var qa=(()=>{class t{_changeDetectorRef=u(De);_elementRef=u(U);_tagName=u(YD);_ngZone=u(H);_focusMonitor=u(Ii);_globalRippleOptions=u(Cd,{optional:!0});_document=u($);_onFocus=new N;_onBlur=new N;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=We();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=u(Ye).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(e){this._value=e}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(e){this._disabled=e}_disabled=!1;removed=new A;destroyed=new A;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=u(om);_injector=u(te);constructor(){let e=u(Ge);e.load($n),e.load(Dn),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=Rt(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this.destroyed.emit({chip:this}),this.destroyed.complete(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(e){(e.keyCode===8&&!e.repeat||e.keyCode===46)&&(e.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(e){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===e||r.contains(e)})}_getActions(){let e=[];return this.editIcon&&e.push(this.editIcon),this.primaryAction&&e.push(this.primaryAction),this.removeIcon&&e.push(this.removeIcon),e}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(e){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{let i=e!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&Wt(o,Xb,5)(o,nT,5)(o,tT,5)(o,iT,5)(o,Xb,5)(o,tT,5)(o,nT,5)(o,iT,5),i&2){let a;K(a=Z())&&(r.leadingIcon=a.first),K(a=Z())&&(r.editIcon=a.first),K(a=Z())&&(r.trailingIcon=a.first),K(a=Z())&&(r.removeIcon=a.first),K(a=Z())&&(r._allLeadingIcons=a),K(a=Z())&&(r._allTrailingIcons=a),K(a=Z())&&(r._allEditIcons=a),K(a=Z())&&(r._allRemoveIcons=a)}},viewQuery:function(i,r){if(i&1&&nt(qz,5),i&2){let o;K(o=Z())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&I("keydown",function(a){return r._handleKeydown(a)}),i&2&&(pt("id",r.id),ee("role",r.role)("aria-label",r.ariaLabel),bt("mat-"+(r.color||"primary")),z("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",j],highlighted:[2,"highlighted","highlighted",j],disableRipple:[2,"disableRipple","disableRipple",j],disabled:[2,"disabled","disabled",j]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[Re([{provide:rT,useExisting:t}])],ngContentSelectors:Hz,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(ke(Uz),de(0,"span",0),m(1,"span",1)(2,"span",2),R(3,zz,2,0,"span",3),m(4,"span",4),se(5),de(6,"span",5),h()()(),R(7,$z,2,0,"span",6)),i&2&&(g(3),O(r.leadingIcon?3:-1),g(4),O(r._hasTrailingIcon()?7:-1))},dependencies:[oT],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-outline-width, 1px);
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-outline-color, var(--%NS%mat-sys-outline));
}
.mdc-evolution-chip__action--%NS%primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--%NS%mat-chip-focus-outline-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--%NS%mat-chip-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-chip-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-chip-label-text-size, var(--%NS%mat-sys-label-large-size));
  font-weight: var(--%NS%mat-chip-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-chip-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  height: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--%NS%selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--%NS%mat-chip-trailing-action-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--%NS%mat-chip-trailing-action-focus-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  height: var(--%NS%mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--%NS%mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  height: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  font-size: var(--%NS%mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --%NS%mat-chip-with-icon-icon-color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-elevated-container-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
  --%NS%mat-chip-label-text-color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-outline-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-hover-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-hover-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--%NS%disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--%NS%mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--%NS%mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--%NS%mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-trailing-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--%NS%mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--%NS%mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-trailing-action-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-selected-trailing-action-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--%NS%mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--%NS%mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2})}return t})();var np=(()=>{class t{_elementRef=u(U);_changeDetectorRef=u(De);_dir=u(Tt,{optional:!0});_lastDestroyedFocusedChipIndex=null;_keyManager;_destroyed=new N;_defaultRole="presentation";get chipFocusChanges(){return this._getChipStream(e=>e._onFocus)}get chipDestroyedChanges(){return this._getChipStream(e=>e.destroyed)}get chipRemovedChanges(){return this._getChipStream(e=>e.removed)}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._syncChipsState()}_disabled=!1;get empty(){return!this._chips||this._chips.length===0}get role(){return this._explicitRole?this._explicitRole:this.empty?null:this._defaultRole}tabIndex=0;set role(e){this._explicitRole=e}_explicitRole=null;get focused(){return this._hasFocusedChip()}_chips;_chipActions=new Si;ngAfterViewInit(){this._setUpFocusManagement(),this._trackChipSetChanges(),this._trackDestroyedFocusedChip()}ngOnDestroy(){this._keyManager?.destroy(),this._chipActions.destroy(),this._destroyed.next(),this._destroyed.complete()}_hasFocusedChip(){return this._chips&&this._chips.some(e=>e._hasFocus())}_syncChipsState(){this._chips?.forEach(e=>{e._chipListDisabled=this._disabled,e._changeDetectorRef.markForCheck()})}focus(){}_handleKeydown(e){this._originatesFromChip(e)&&this._keyManager.onKeydown(e)}_isValidIndex(e){return e>=0&&e<this._chips.length}_allowFocusEscape(){let e=this._elementRef.nativeElement.tabIndex;e!==-1&&(this._elementRef.nativeElement.tabIndex=-1,setTimeout(()=>this._elementRef.nativeElement.tabIndex=e))}_getChipStream(e){return this._chips.changes.pipe(dt(null),ht(()=>Rt(...this._chips.map(e))))}_originatesFromChip(e){let i=e.target;for(;i&&i!==this._elementRef.nativeElement;){if(i.classList.contains("mat-mdc-chip"))return!0;i=i.parentElement}return!1}_setUpFocusManagement(){this._chips.changes.pipe(dt(this._chips)).subscribe(e=>{let i=[];e.forEach(r=>r._getActions().forEach(o=>i.push(o))),this._chipActions.reset(i),this._chipActions.notifyOnChanges()}),this._keyManager=new Pa(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir?this._dir.value:"ltr").withHomeAndEnd().skipPredicate(e=>this._skipPredicate(e)),this.chipFocusChanges.pipe(Ee(this._destroyed)).subscribe(({chip:e})=>{let i=e._getSourceAction(document.activeElement);i&&this._keyManager.updateActiveItem(i)}),this._dir?.change.pipe(Ee(this._destroyed)).subscribe(e=>this._keyManager.withHorizontalOrientation(e))}_skipPredicate(e){return e.disabled}_trackChipSetChanges(){this._chips.changes.pipe(dt(null),Ee(this._destroyed)).subscribe(()=>{this.disabled&&Promise.resolve().then(()=>this._syncChipsState()),this._redirectDestroyedChipFocus()})}_trackDestroyedFocusedChip(){this.chipDestroyedChanges.pipe(Ee(this._destroyed)).subscribe(e=>{let r=this._chips.toArray().indexOf(e.chip),o=e.chip._hasFocus(),a=e.chip._hadFocusOnRemove&&this._keyManager.activeItem&&e.chip._getActions().includes(this._keyManager.activeItem),s=o||a;this._isValidIndex(r)&&s&&(this._lastDestroyedFocusedChipIndex=r)})}_redirectDestroyedChipFocus(){if(this._lastDestroyedFocusedChipIndex!=null){if(this._chips.length){let e=Math.min(this._lastDestroyedFocusedChipIndex,this._chips.length-1),i=this._chips.toArray()[e];i.disabled?this._chips.length===1?this.focus():this._keyManager.setPreviousItemActive():i.focus()}else this.focus();this._lastDestroyedFocusedChipIndex=null}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-chip-set"]],contentQueries:function(i,r,o){if(i&1&&Wt(o,qa,5),i&2){let a;K(a=Z())&&(r._chips=a)}},hostAttrs:[1,"mat-mdc-chip-set","mdc-evolution-chip-set"],hostVars:1,hostBindings:function(i,r){i&1&&I("keydown",function(a){return r._handleKeydown(a)}),i&2&&ee("role",r.role)},inputs:{disabled:[2,"disabled","disabled",j],role:"role",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:ir(e)]},ngContentSelectors:Gz,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(i,r){i&1&&(ke(),Ve(0,"div",0),se(1),$e())},styles:[`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 4px 0 4px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`],encapsulation:2})}return t})();var wc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({providers:[Mo,{provide:Wz,useValue:{separatorKeyCodes:[13]}}],imports:[Co,Ne]})}return t})();var Yz=t=>({returnUrl:t}),Kz=()=>({}),Zz=(t,n)=>n.typeId;function Qz(t,n){t&1&&(m(0,"p"),v(1,"U\u010Ditavanje tipova..."),h())}function Xz(t,n){t&1&&(m(0,"mat-icon",19),v(1,"check"),h())}function Jz(t,n){if(t&1){let e=Ie();m(0,"mat-chip",18),I("click",function(){let r=q(e).$implicit,o=D(2);return Y(o.toggleFavoriteType(r.typeId))}),R(1,Xz,2,0,"mat-icon",19),v(2),h()}if(t&2){let e=n.$implicit,i=D(2);T("highlighted",i.selectedTypeIds().has(e.typeId)),g(),O(i.selectedTypeIds().has(e.typeId)?1:-1),g(),pe(" ",e.name," ")}}function e3(t,n){if(t&1&&(m(0,"mat-chip-set"),at(1,Jz,3,3,"mat-chip",17,Zz),h()),t&2){let e=D();g(),st(e.types())}}function t3(t,n){if(t&1&&(m(0,"p",13),v(1),h()),t&2){let e=D();g(),G(e.error())}}var ip=class t{constructor(n,e,i){this.router=n;this.route=e;this.location=i;this.returnUrl=this.route.snapshot.queryParamMap.get("returnUrl")}router;route;location;firstName=S("");lastName=S("");email=S("");phone=S("");address=S("");password=S("");showPassword=S(!1);error=S(null);loading=S(!1);returnUrl=null;types=S([]);loadingTypes=S(!0);selectedTypeIds=S(new Set);ngOnInit(){this.loadTypes()}async loadTypes(){this.loadingTypes.set(!0);try{let n=await pn.getTypes();this.types.set(n.data)}catch(n){console.error("Gre\u0161ka pri u\u010Ditavanju tipova igra\u010Daka",n)}finally{this.loadingTypes.set(!1)}}toggleFavoriteType(n){let e=new Set(this.selectedTypeIds());e.has(n)?e.delete(n):e.add(n),this.selectedTypeIds.set(e)}goBack(){this.location.back()}toggleShowPassword(){this.showPassword.set(!this.showPassword())}async onSubmit(){this.error.set(null),this.loading.set(!0);try{await Ke.signup(this.email(),this.password(),this.firstName(),this.lastName(),this.phone(),this.address(),Array.from(this.selectedTypeIds())),this.router.navigateByUrl(this.returnUrl??"/")}catch(n){this.error.set(n instanceof Error?n.message:"Registracija nije uspela.")}finally{this.loading.set(!1)}}static \u0275fac=function(e){return new(e||t)(ie(qt),ie(mn),ie(ai))};static \u0275cmp=F({type:t,selectors:[["app-signup"]],decls:48,vars:17,consts:[[1,"auth-page"],[1,"auth-card"],["mat-button","","type","button",1,"back-link",3,"click"],["autocomplete","off",3,"ngSubmit"],["appearance","outline",1,"full-width"],["matInput","","name","firstName","autocomplete","off","required","",3,"ngModelChange","ngModel"],["matInput","","name","lastName","autocomplete","off","required","",3,"ngModelChange","ngModel"],["matInput","","type","email","name","email","autocomplete","off","required","",3,"ngModelChange","ngModel"],["matInput","","type","tel","name","phone","autocomplete","off","required","",3,"ngModelChange","ngModel"],["matInput","","name","address","autocomplete","off","required","",3,"ngModelChange","ngModel"],["matInput","","name","password","autocomplete","new-password","required","",3,"ngModelChange","type","ngModel"],["mat-icon-button","","matSuffix","","type","button",3,"click"],[1,"favorites-section"],[1,"error"],["mat-raised-button","","color","primary","type","submit",1,"full-width",3,"disabled"],[1,"switch-link"],["routerLink","/login",3,"queryParams"],[3,"highlighted"],[3,"click","highlighted"],["matChipAvatar",""]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card",1)(2,"button",2),I("click",function(){return i.goBack()}),m(3,"mat-icon"),v(4,"arrow_back"),h(),v(5," Nazad "),h(),m(6,"h1"),v(7,"Registracija"),h(),m(8,"form",3),I("ngSubmit",function(){return i.onSubmit()}),m(9,"mat-form-field",4)(10,"mat-label"),v(11,"Ime"),h(),m(12,"input",5),I("ngModelChange",function(o){return i.firstName.set(o)}),h(),Fe(),h(),m(13,"mat-form-field",4)(14,"mat-label"),v(15,"Prezime"),h(),m(16,"input",6),I("ngModelChange",function(o){return i.lastName.set(o)}),h(),Fe(),h(),m(17,"mat-form-field",4)(18,"mat-label"),v(19,"Email"),h(),m(20,"input",7),I("ngModelChange",function(o){return i.email.set(o)}),h(),Fe(),h(),m(21,"mat-form-field",4)(22,"mat-label"),v(23,"Telefon"),h(),m(24,"input",8),I("ngModelChange",function(o){return i.phone.set(o)}),h(),Fe(),h(),m(25,"mat-form-field",4)(26,"mat-label"),v(27,"Adresa"),h(),m(28,"input",9),I("ngModelChange",function(o){return i.address.set(o)}),h(),Fe(),h(),m(29,"mat-form-field",4)(30,"mat-label"),v(31,"Lozinka"),h(),m(32,"input",10),I("ngModelChange",function(o){return i.password.set(o)}),h(),Fe(),m(33,"button",11),I("click",function(){return i.toggleShowPassword()}),m(34,"mat-icon"),v(35),h()()(),m(36,"div",12)(37,"h2"),v(38,"Omiljeni tipovi igra\u010Daka"),h(),R(39,Qz,2,0,"p")(40,e3,3,0,"mat-chip-set"),h(),R(41,t3,2,1,"p",13),m(42,"button",14),v(43),h()(),m(44,"p",15),v(45," Ve\u0107 imate nalog? "),m(46,"a",16),v(47,"Prijavite se"),h()()()()),e&2&&(g(12),T("ngModel",i.firstName()),Le(),g(4),T("ngModel",i.lastName()),Le(),g(4),T("ngModel",i.email()),Le(),g(4),T("ngModel",i.phone()),Le(),g(4),T("ngModel",i.address()),Le(),g(4),T("type",i.showPassword()?"text":"password")("ngModel",i.password()),Le(),g(),ee("aria-label",i.showPassword()?"Sakrij lozinku":"Prika\u017Ei lozinku"),g(2),G(i.showPassword()?"visibility_off":"visibility"),g(4),O(i.loadingTypes()?39:40),g(2),O(i.error()?41:-1),g(),T("disabled",i.loading()),g(),pe(" ",i.loading()?"Registracija...":"Registruj se"," "),g(3),T("queryParams",i.returnUrl?Ir(14,Yz,i.returnUrl):mo(16,Kz)))},dependencies:[pi,Fm,Tn,mi,Pm,dc,Gn,Ri,Ht,xn,En,gn,gi,Wn,Vr,lr,cr,Vt,St,or,In,Mn,wc,qa,tp,np],styles:[".auth-page[_ngcontent-%COMP%]{display:flex;justify-content:center;padding:48px 24px}.auth-card[_ngcontent-%COMP%]{width:100%;max-width:400px;padding:24px}.auth-card[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0 0 24px;text-align:center}.full-width[_ngcontent-%COMP%]{width:100%;margin-bottom:8px}.error[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-error);font-size:14px;margin:0 0 12px}.switch-link[_ngcontent-%COMP%]{text-align:center;margin-top:16px;font-size:14px}.favorites-section[_ngcontent-%COMP%]{margin:8px 0 20px}.favorites-section[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{font-size:16px;margin:0 0 12px}mat-chip[_ngcontent-%COMP%]{cursor:pointer}"]})};var n3=(t,n)=>n.typeId;function i3(t,n){if(t&1){let e=Ie();m(0,"button",18),I("click",function(){q(e);let r=D();return Y(r.togglePasswordForm())}),m(1,"mat-icon"),v(2,"lock_reset"),h(),v(3," Promeni lozinku "),h()}}function r3(t,n){if(t&1){let e=Ie();m(0,"div",2)(1,"mat-form-field",3)(2,"mat-label"),v(3,"Nova lozinka"),h(),m(4,"input",19),I("ngModelChange",function(r){q(e);let o=D();return Y(o.newPassword.set(r))}),h(),Fe(),m(5,"button",20),I("click",function(){q(e);let r=D();return Y(r.toggleShowNewPassword())}),m(6,"mat-icon"),v(7),h()()(),m(8,"mat-form-field",3)(9,"mat-label"),v(10,"Potvrda nove lozinke"),h(),m(11,"input",21),I("ngModelChange",function(r){q(e);let o=D();return Y(o.confirmPassword.set(r))}),h(),Fe(),m(12,"button",20),I("click",function(){q(e);let r=D();return Y(r.toggleShowConfirmPassword())}),m(13,"mat-icon"),v(14),h()()()(),m(15,"button",22),I("click",function(){q(e);let r=D();return Y(r.togglePasswordForm())}),v(16,"Otka\u017Ei promenu lozinke"),h()}if(t&2){let e=D();g(4),T("type",e.showNewPassword()?"text":"password")("ngModel",e.newPassword()),Le(),g(),ee("aria-label",e.showNewPassword()?"Sakrij lozinku":"Prika\u017Ei lozinku"),g(2),G(e.showNewPassword()?"visibility_off":"visibility"),g(4),T("type",e.showConfirmPassword()?"text":"password")("ngModel",e.confirmPassword()),Le(),g(),ee("aria-label",e.showConfirmPassword()?"Sakrij lozinku":"Prika\u017Ei lozinku"),g(2),G(e.showConfirmPassword()?"visibility_off":"visibility")}}function o3(t,n){t&1&&(m(0,"p"),v(1,"U\u010Ditavanje tipova..."),h())}function a3(t,n){t&1&&(m(0,"mat-icon",25),v(1,"check"),h())}function s3(t,n){if(t&1){let e=Ie();m(0,"mat-chip",24),I("click",function(){let r=q(e).$implicit,o=D(2);return Y(o.toggleFavoriteType(r.typeId))}),R(1,a3,2,0,"mat-icon",25),v(2),h()}if(t&2){let e=n.$implicit,i=D(2);T("highlighted",i.selectedTypeIds().has(e.typeId)),g(),O(i.selectedTypeIds().has(e.typeId)?1:-1),g(),pe(" ",e.name," ")}}function c3(t,n){if(t&1&&(m(0,"mat-chip-set"),at(1,s3,3,3,"mat-chip",23,n3),h()),t&2){let e=D();g(),st(e.types())}}function l3(t,n){if(t&1&&(m(0,"p",15),v(1),h()),t&2){let e=D();g(),G(e.saveMessage())}}function d3(t,n){if(t&1&&(m(0,"p",16),v(1),h()),t&2){let e=D();g(),G(e.errorMessage())}}var rp=class t{firstName=S("");lastName=S("");email=S("");phone=S("");address=S("");selectedTypeIds=S(new Set);types=S([]);loadingTypes=S(!0);showPasswordForm=S(!1);newPassword=S("");confirmPassword=S("");showNewPassword=S(!1);showConfirmPassword=S(!1);saving=S(!1);saveMessage=S(null);errorMessage=S(null);userId="";ngOnInit(){let n=Ke.getCurrentUser();n&&(this.userId=n.userId,this.firstName.set(n.firstName),this.lastName.set(n.lastName),this.email.set(n.email),this.phone.set(n.phone),this.address.set(n.address),this.selectedTypeIds.set(new Set(n.favoriteToyTypes??[])),this.loadTypes())}async loadTypes(){this.loadingTypes.set(!0);try{let n=await pn.getTypes();this.types.set(n.data)}catch(n){console.error("Gre\u0161ka pri u\u010Ditavanju tipova igra\u010Daka",n)}finally{this.loadingTypes.set(!1)}}toggleFavoriteType(n){let e=new Set(this.selectedTypeIds());e.has(n)?e.delete(n):e.add(n),this.selectedTypeIds.set(e)}togglePasswordForm(){this.showPasswordForm.set(!this.showPasswordForm()),this.newPassword.set(""),this.confirmPassword.set(""),this.showNewPassword.set(!1),this.showConfirmPassword.set(!1)}toggleShowNewPassword(){this.showNewPassword.set(!this.showNewPassword())}toggleShowConfirmPassword(){this.showConfirmPassword.set(!this.showConfirmPassword())}async onSave(){if(this.errorMessage.set(null),this.saveMessage.set(null),this.showPasswordForm()&&this.newPassword()){if(this.newPassword().length<6){this.errorMessage.set("Nova lozinka mora imati bar 6 karaktera.");return}if(this.newPassword()!==this.confirmPassword()){this.errorMessage.set("Lozinke se ne poklapaju.");return}}this.saving.set(!0);try{Ke.updateProfile(this.userId,{firstName:this.firstName(),lastName:this.lastName(),email:this.email(),phone:this.phone(),address:this.address(),favoriteToyTypes:Array.from(this.selectedTypeIds())}),this.showPasswordForm()&&this.newPassword()&&(await Ke.changePassword(this.userId,this.newPassword()),this.showPasswordForm.set(!1),this.newPassword.set(""),this.confirmPassword.set("")),this.saveMessage.set("Podaci su uspe\u0161no sa\u010Duvani.")}catch(n){this.errorMessage.set(n instanceof Error?n.message:"\u010Cuvanje nije uspelo.")}finally{this.saving.set(!1)}}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=F({type:t,selectors:[["app-profile"]],decls:48,vars:11,consts:[[1,"profile-page"],[1,"profile-card"],[1,"field-row","two-col"],["appearance","outline",1,"full-width"],["matInput","","name","firstName",3,"ngModelChange","ngModel"],["matInput","","name","lastName",3,"ngModelChange","ngModel"],["matInput","","name","phone",3,"ngModelChange","ngModel"],["matInput","","name","address",3,"ngModelChange","ngModel"],[1,"login-data-section"],[1,"field-row"],["matInput","","type","email","name","email",3,"ngModelChange","ngModel"],[1,"section-hint"],["matInput","","type","password","value","\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022","disabled",""],["mat-stroked-button","","type","button",1,"change-password-button"],[1,"favorites-section"],[1,"success"],[1,"error"],["mat-flat-button","","color","primary",1,"save-button",3,"click","disabled"],["mat-stroked-button","","type","button",1,"change-password-button",3,"click"],["matInput","","name","newPassword",3,"ngModelChange","type","ngModel"],["mat-icon-button","","matSuffix","","type","button",3,"click"],["matInput","","name","confirmPassword",3,"ngModelChange","type","ngModel"],["mat-button","","type","button",3,"click"],[3,"highlighted"],[3,"click","highlighted"],["matChipAvatar",""]],template:function(e,i){e&1&&(m(0,"div",0)(1,"mat-card",1)(2,"h1"),v(3,"Profil"),h(),m(4,"div",2)(5,"mat-form-field",3)(6,"mat-label"),v(7,"Ime"),h(),m(8,"input",4),I("ngModelChange",function(o){return i.firstName.set(o)}),h(),Fe(),h(),m(9,"mat-form-field",3)(10,"mat-label"),v(11,"Prezime"),h(),m(12,"input",5),I("ngModelChange",function(o){return i.lastName.set(o)}),h(),Fe(),h()(),m(13,"div",2)(14,"mat-form-field",3)(15,"mat-label"),v(16,"Telefon"),h(),m(17,"input",6),I("ngModelChange",function(o){return i.phone.set(o)}),h(),Fe(),h(),m(18,"mat-form-field",3)(19,"mat-label"),v(20,"Adresa"),h(),m(21,"input",7),I("ngModelChange",function(o){return i.address.set(o)}),h(),Fe(),h()(),m(22,"div",8)(23,"h2"),v(24,"Podaci za prijavu"),h(),m(25,"div",9)(26,"mat-form-field",3)(27,"mat-label"),v(28,"Email"),h(),m(29,"input",10),I("ngModelChange",function(o){return i.email.set(o)}),h(),Fe(),h()(),m(30,"p",11),v(31,"Lozinka se iz bezbednosnih razloga ne prikazuje."),h(),m(32,"div",9)(33,"mat-form-field",3)(34,"mat-label"),v(35,"Lozinka"),h(),de(36,"input",12),h()(),R(37,i3,4,0,"button",13)(38,r3,17,8),h(),m(39,"div",14)(40,"h2"),v(41,"Omiljeni tipovi igra\u010Daka"),h(),R(42,o3,2,0,"p")(43,c3,3,0,"mat-chip-set"),h(),R(44,l3,2,1,"p",15),R(45,d3,2,1,"p",16),m(46,"button",17),I("click",function(){return i.onSave()}),v(47),h()()()),e&2&&(g(8),T("ngModel",i.firstName()),Le(),g(4),T("ngModel",i.lastName()),Le(),g(5),T("ngModel",i.phone()),Le(),g(4),T("ngModel",i.address()),Le(),g(8),T("ngModel",i.email()),Le(),g(8),O(i.showPasswordForm()?38:37),g(5),O(i.loadingTypes()?42:43),g(2),O(i.saveMessage()?44:-1),g(),O(i.errorMessage()?45:-1),g(),T("disabled",i.saving()),g(),pe(" ",i.saving()?"\u010Cuvanje...":"Sa\u010Duvaj izmene"," "))},dependencies:[pi,Tn,mi,Gn,xn,En,gn,gi,Wn,Vr,lr,cr,Vt,St,or,wc,qa,tp,np,In,Mn],styles:[".profile-page[_ngcontent-%COMP%]{display:flex;justify-content:center;padding:48px 24px}.profile-card[_ngcontent-%COMP%]{width:100%;max-width:560px;padding:32px}.profile-card[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0 0 24px}.field-row[_ngcontent-%COMP%]{margin-bottom:8px}.two-col[_ngcontent-%COMP%]{display:flex;gap:16px}.two-col[_ngcontent-%COMP%]   .full-width[_ngcontent-%COMP%]{flex:1}.full-width[_ngcontent-%COMP%]{width:100%}.favorites-section[_ngcontent-%COMP%]{margin:16px 0 24px}.favorites-section[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{font-size:16px;margin:0 0 12px}mat-chip[_ngcontent-%COMP%]{cursor:pointer}.login-data-section[_ngcontent-%COMP%]{margin:8px 0 24px;padding-top:16px;border-top:1px solid var(--%NS%mat-sys-outline-variant)}.login-data-section[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{font-size:16px;margin:0 0 16px}.section-hint[_ngcontent-%COMP%]{font-size:12px;color:var(--%NS%mat-sys-on-surface-variant);margin:0 0 12px}.success[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-primary);font-size:14px;margin:0 0 12px}.error[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-error);font-size:14px;margin:0 0 12px}.change-password-button[_ngcontent-%COMP%]{background-color:var(--%NS%mat-sys-primary-container);color:var(--%NS%mat-sys-on-primary-container);border-color:var(--%NS%mat-sys-primary)}.save-button[_ngcontent-%COMP%]{width:100%;margin-top:16px;padding:8px 0;font-size:15px}"]})};var u3=(t,n)=>n.reservationId;function f3(t,n){if(t&1&&(m(0,"p",1),v(1),h()),t&2){let e=D();g(),G(e.errorMessage())}}function h3(t,n){t&1&&(m(0,"p",2),v(1," Korpa je prazna. Pogledajte "),m(2,"a",3),v(3,"igra\u010Dke"),h(),v(4," i napravite rezervaciju. "),h())}function m3(t,n){if(t&1){let e=Ie();m(0,"div",19)(1,"button",25),I("click",function(){q(e);let r=D().$implicit,o=D(2);return Y(o.decreaseQuantity(r))}),v(2," \u2212 "),h(),m(3,"span",26),v(4),h(),m(5,"button",27),I("click",function(){q(e);let r=D().$implicit,o=D(2);return Y(o.increaseQuantity(r))}),v(6," + "),h()()}if(t&2){let e=D().$implicit;g(),T("disabled",e.quantity<=1),g(3),G(e.quantity)}}function p3(t,n){if(t&1&&(m(0,"span",20),v(1),h()),t&2){let e=D().$implicit;g(),pe("Koli\u010Dina: ",e.quantity)}}function g3(t,n){if(t&1&&(m(0,"mat-icon"),v(1),h()),t&2){let e=n.$implicit,i=D(3).$implicit;g(),G(i.rating>=e?"star":"star_border")}}function _3(t,n){if(t&1&&(m(0,"div",28)(1,"span"),v(2,"Va\u0161a ocena:"),h(),at(3,g3,2,1,"mat-icon",null,_a),h()),t&2){let e=D(4);g(3),st(e.stars)}}function v3(t,n){if(t&1){let e=Ie();m(0,"mat-icon",40),I("click",function(){let r=q(e).$implicit,o=D(5);return Y(o.setNewRating(r))}),v(1),h()}if(t&2){let e=n.$implicit,i=D(5);g(),pe(" ",i.newRating()>=e?"star":"star_border"," ")}}function y3(t,n){if(t&1&&(m(0,"p",1),v(1),h()),t&2){let e=D(5);g(),G(e.ratingError())}}function b3(t,n){if(t&1){let e=Ie();m(0,"mat-card",29)(1,"div",30)(2,"span"),v(3,"Ocena:"),h(),at(4,v3,2,1,"mat-icon",31,_a),h(),m(6,"mat-form-field",32)(7,"mat-label"),v(8,"Recenziju ostavlja"),h(),m(9,"mat-select",33),I("ngModelChange",function(r){q(e);let o=D(4);return Y(o.newReviewAuthorType.set(r))}),m(10,"mat-option",34),v(11,"Roditelj"),h(),m(12,"mat-option",35),v(13,"Dete"),h()(),Fe(),h(),m(14,"mat-form-field",32)(15,"mat-label"),v(16,"Komentar"),h(),m(17,"textarea",36),I("ngModelChange",function(r){q(e);let o=D(4);return Y(o.newReviewComment.set(r))}),h(),Fe(),h(),R(18,y3,2,1,"p",1),m(19,"div",37)(20,"button",38),I("click",function(){q(e);let r=D(2).$implicit,o=D(2);return Y(o.submitRating(r))}),v(21),h(),m(22,"button",39),I("click",function(){q(e);let r=D(4);return Y(r.closeRatingForm())}),v(23,"Otka\u017Ei"),h()()()}if(t&2){let e=D(4);g(4),st(e.stars),g(5),T("ngModel",e.newReviewAuthorType()),Le(),g(8),T("ngModel",e.newReviewComment()),Le(),g(),O(e.ratingError()?18:-1),g(2),T("disabled",e.submittingRating()),g(),pe(" ",e.submittingRating()?"Slanje...":"Sa\u010Duvaj ocenu"," ")}}function C3(t,n){if(t&1&&R(0,_3,5,0,"div",28)(1,b3,24,5,"mat-card",29),t&2){let e=D().$implicit,i=D(2);O(e.rating!=null?0:i.ratingForReservationId()===e.reservationId?1:-1)}}function w3(t,n){if(t&1){let e=Ie();m(0,"button",41),I("click",function(){q(e);let r=D().$implicit,o=D(2);return Y(o.markAsArrived(r.reservationId))}),m(1,"mat-icon"),v(2,"local_shipping"),h(),v(3," Ozna\u010Di kao pristiglo "),h(),m(4,"button",42),I("click",function(){q(e);let r=D().$implicit,o=D(2);return Y(o.cancelReservation(r.reservationId))}),v(5," Otka\u017Ei rezervaciju "),h()}}function S3(t,n){if(t&1){let e=Ie();m(0,"button",45),I("click",function(){q(e);let r=D(2).$implicit,o=D(2);return Y(o.openRatingForm(r.reservationId))}),m(1,"mat-icon"),v(2,"star_rate"),h(),v(3," Oceni igra\u010Dku "),h()}}function D3(t,n){if(t&1){let e=Ie();R(0,S3,4,0,"button",43),m(1,"button",44),I("click",function(){q(e);let r=D().$implicit,o=D(2);return Y(o.removeFromCart(r.reservationId))}),v(2," Ukloni iz korpe "),h()}if(t&2){let e=D().$implicit,i=D(2);O(e.rating==null&&i.ratingForReservationId()!==e.reservationId?0:-1)}}function E3(t,n){if(t&1){let e=Ie();m(0,"button",44),I("click",function(){q(e);let r=D().$implicit,o=D(2);return Y(o.removeFromCart(r.reservationId))}),v(1," Ukloni iz korpe "),h()}}function x3(t,n){if(t&1&&(m(0,"mat-card",8),de(1,"img",9),m(2,"div",10)(3,"div",11)(4,"h3"),v(5),h(),m(6,"mat-chip"),v(7),h()(),m(8,"p",12),v(9),h(),m(10,"div",13)(11,"div",14)(12,"span",15),v(13,"Tip:"),h(),m(14,"span"),v(15),h()(),m(16,"div",14)(17,"span",15),v(18,"Uzrast:"),h(),m(19,"span"),v(20),h()(),m(21,"div",14)(22,"span",15),v(23,"Ciljna grupa:"),h(),m(24,"span"),v(25),h()(),m(26,"div",14)(27,"span",15),v(28,"Datum proizvodnje:"),h(),m(29,"span"),v(30),ba(31,"date"),h()()(),m(32,"p",16),v(33),ba(34,"date"),h(),m(35,"div",17)(36,"span",18),v(37),h(),R(38,m3,7,2,"div",19)(39,p3,2,1,"span",20),m(40,"span",21)(41,"span",22),v(42,"Ukupno:"),h(),v(43),h()(),R(44,C3,2,1),m(45,"div",23),R(46,w3,6,0),R(47,D3,3,1),R(48,E3,2,0,"button",24),h()()()),t&2){let e=n.$implicit,i=D(2);z("reservation-cancelled",e.status==="otkazano"),g(),T("src",i.imageBaseUrl+e.toyImageUrl,tr)("alt",e.toyName),g(4),G(e.toyName),g(),z("status-rezervisano",e.status==="rezervisano")("status-pristiglo",e.status==="pristiglo")("status-otkazano",e.status==="otkazano"),g(),pe(" ",i.statusLabel(e.status)," "),g(2),G(e.toyDescription),g(6),G(e.toyTypeName),g(5),G(e.toyAgeGroupName),g(5),G(i.targetGroupLabel(e.toyTargetGroup)),g(5),G(Ca(31,25,e.toyProductionDate,"dd.MM.yyyy.")),g(3),pe(" Rezervisano: ",Ca(34,28,e.reservationDate,"dd.MM.yyyy.")," "),g(4),pe("",e.toyPrice," RSD / kom."),g(),O(e.status==="rezervisano"?38:39),g(5),pe(" ",i.lineTotal(e)," RSD"),g(),O(e.status==="pristiglo"?44:-1),g(2),O(e.status==="rezervisano"?46:-1),g(),O(e.status==="pristiglo"?47:-1),g(),O(e.status==="otkazano"?48:-1)}}function M3(t,n){if(t&1&&(m(0,"div",4),at(1,x3,49,31,"mat-card",5,u3),h(),m(3,"div",6)(4,"span"),v(5,"Ukupno:"),h(),m(6,"span",7),v(7),h()()),t&2){let e=D();g(),st(e.reservations()),g(6),pe("",e.totalPrice()," RSD")}}var op=class t{stars=[1,2,3,4,5];targetGroupLabel=Xm;imageBaseUrl="https://toy.pequla.com";reservations=S([]);errorMessage=S(null);totalPrice=ct(()=>this.reservations().filter(n=>n.status!=="otkazano").reduce((n,e)=>n+e.toyPrice*e.quantity,0));ratingForReservationId=S(null);newRating=S(0);newReviewAuthorType=S("roditelj");newReviewComment=S("");ratingError=S(null);submittingRating=S(!1);userId="";ngOnInit(){let n=Ke.getCurrentUser();n&&(this.userId=n.userId,this.loadReservations())}loadReservations(){let n=Ke.getReservationsForUser(this.userId);n.sort((e,i)=>{let r=e.status==="otkazano"?1:0,o=i.status==="otkazano"?1:0;return r!==o?r-o:new Date(i.reservationDate).getTime()-new Date(e.reservationDate).getTime()}),this.reservations.set(n)}cancelReservation(n){this.errorMessage.set(null);try{Ke.updateReservationStatus(n,"otkazano"),this.loadReservations()}catch(e){this.errorMessage.set(e instanceof Error?e.message:"Otkazivanje nije uspelo.")}}markAsArrived(n){this.errorMessage.set(null);try{Ke.markReservationArrived(n),this.loadReservations()}catch(e){this.errorMessage.set(e instanceof Error?e.message:"Ozna\u010Davanje kao pristiglo nije uspelo.")}}removeFromCart(n){this.errorMessage.set(null);try{Ke.deleteReservation(n),this.loadReservations()}catch(e){this.errorMessage.set(e instanceof Error?e.message:"Brisanje nije uspelo.")}}lineTotal(n){return n.toyPrice*n.quantity}increaseQuantity(n){this.setQuantity(n,n.quantity+1)}decreaseQuantity(n){this.setQuantity(n,n.quantity-1)}setQuantity(n,e){if(!(e<1)){this.errorMessage.set(null);try{Ke.updateReservationQuantity(n.reservationId,e),this.loadReservations()}catch(i){this.errorMessage.set(i instanceof Error?i.message:"Izmena koli\u010Dine nije uspela.")}}}openRatingForm(n){this.ratingError.set(null),this.ratingForReservationId.set(n),this.newRating.set(0),this.newReviewAuthorType.set("roditelj"),this.newReviewComment.set("")}closeRatingForm(){this.ratingForReservationId.set(null)}setNewRating(n){this.newRating.set(n)}submitRating(n){if(this.ratingError.set(null),this.newRating()<1){this.ratingError.set("Izaberite ocenu od 1 do 5 zvezdica.");return}if(!this.newReviewComment().trim()){this.ratingError.set("Unesite kratak komentar.");return}this.submittingRating.set(!0);try{Ke.rateReservation(n.reservationId,this.newRating()),zr.addReview(n.toyId,this.newReviewAuthorType(),this.newRating(),this.newReviewComment()),this.closeRatingForm(),this.loadReservations()}catch(e){this.ratingError.set(e instanceof Error?e.message:"Ocenjivanje nije uspelo.")}finally{this.submittingRating.set(!1)}}statusLabel(n){switch(n){case"rezervisano":return"Rezervisano";case"pristiglo":return"Pristiglo";case"otkazano":return"Otkazano"}}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=F({type:t,selectors:[["app-cart"]],decls:6,vars:2,consts:[[1,"cart-page"],[1,"error"],[1,"empty-cart"],["routerLink","/igracke"],[1,"reservation-list"],[1,"reservation-card",3,"reservation-cancelled"],[1,"cart-summary"],[1,"total-price"],[1,"reservation-card"],[1,"reservation-image",3,"src","alt"],[1,"reservation-body"],[1,"reservation-header"],[1,"reservation-description"],[1,"meta-grid"],[1,"meta-item"],[1,"meta-label"],[1,"reservation-date"],[1,"reservation-price-row"],[1,"unit-price"],[1,"quantity-control"],[1,"quantity-readonly"],[1,"line-total"],[1,"line-total-label"],[1,"reservation-actions"],["mat-stroked-button","",1,"remove-button"],["type","button","aria-label","Smanji koli\u010Dinu",1,"qty-btn",3,"click","disabled"],[1,"quantity-value"],["type","button","aria-label","Pove\u0107aj koli\u010Dinu",1,"qty-btn",3,"click"],[1,"rating-row","rating-row-readonly"],[1,"rating-form"],[1,"rating-row"],[1,"star"],["appearance","outline",1,"full-width"],[3,"ngModelChange","ngModel"],["value","roditelj"],["value","dete"],["matInput","","rows","2",3,"ngModelChange","ngModel"],[1,"rating-form-actions"],["mat-raised-button","","color","primary",3,"click","disabled"],["mat-button","",3,"click"],[1,"star",3,"click"],["mat-stroked-button","",3,"click"],["mat-stroked-button","","color","warn",3,"click"],["mat-stroked-button","",1,"rate-button"],["mat-stroked-button","",1,"remove-button",3,"click"],["mat-stroked-button","",1,"rate-button",3,"click"]],template:function(e,i){e&1&&(m(0,"div",0)(1,"h1"),v(2,"Korpa"),h(),R(3,f3,2,1,"p",1),R(4,h3,5,0,"p",2)(5,M3,8,1),h()),e&2&&(g(3),O(i.errorMessage()?3:-1),g(),O(i.reservations().length===0?4:5))},dependencies:[Ht,xn,En,Vt,St,In,Mn,wc,qa,gn,gi,Wn,qm,Wm,To,lr,cr,pi,Tn,mi,Gn,jl],styles:[".cart-page[_ngcontent-%COMP%]{max-width:800px;margin:0 auto;padding:24px}.error[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-error);font-size:14px}.empty-cart[_ngcontent-%COMP%]{padding:32px 0;text-align:center}.reservation-list[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:16px}.reservation-card[_ngcontent-%COMP%]{display:flex;gap:16px;padding:16px}.reservation-image[_ngcontent-%COMP%]{width:120px;height:120px;object-fit:cover;border-radius:8px;flex-shrink:0}.reservation-body[_ngcontent-%COMP%]{flex:1;display:flex;flex-direction:column;gap:4px}.reservation-header[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center}.reservation-header[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0}.reservation-description[_ngcontent-%COMP%]{margin:4px 0;font-size:14px;color:var(--%NS%mat-sys-on-surface-variant)}.meta-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(2,1fr);gap:4px 24px;margin:8px 0}.meta-item[_ngcontent-%COMP%]{display:flex;gap:4px;font-size:13px}.meta-label[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-surface-variant);font-weight:600}.reservation-date[_ngcontent-%COMP%]{margin:0;font-size:13px;color:var(--%NS%mat-sys-on-surface-variant)}.line-total[_ngcontent-%COMP%]{margin:0 0 0 auto;font-weight:600}.line-total-label[_ngcontent-%COMP%]{font-weight:400;color:var(--%NS%mat-sys-on-surface-variant);margin-right:4px}.reservation-price-row[_ngcontent-%COMP%]{display:flex;align-items:center;gap:16px;margin:4px 0;flex-wrap:wrap}.unit-price[_ngcontent-%COMP%]{font-size:13px;color:var(--%NS%mat-sys-on-surface-variant)}.quantity-control[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:10px}.qty-btn[_ngcontent-%COMP%]{width:26px;height:26px;padding:0;border:1px solid var(--%NS%mat-sys-outline-variant);border-radius:50%;background:var(--%NS%mat-sys-surface-container-low);color:var(--%NS%mat-sys-on-surface);font-size:16px;line-height:1;display:flex;align-items:center;justify-content:center;cursor:pointer}.qty-btn[_ngcontent-%COMP%]:hover:not(:disabled){background:var(--%NS%mat-sys-surface-container-high)}.qty-btn[_ngcontent-%COMP%]:disabled{opacity:.4;cursor:not-allowed}.quantity-value[_ngcontent-%COMP%]{min-width:16px;text-align:center;font-weight:600}.quantity-readonly[_ngcontent-%COMP%]{font-size:13px;color:var(--%NS%mat-sys-on-surface-variant)}.rating-row[_ngcontent-%COMP%]{display:flex;align-items:center;gap:4px;margin-top:4px}.rating-row[_ngcontent-%COMP%]   .star[_ngcontent-%COMP%]{cursor:pointer;color:var(--%NS%mat-sys-tertiary)}.rating-row-readonly[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-tertiary)}.rating-form[_ngcontent-%COMP%]{padding:12px;margin-top:8px;display:flex;flex-direction:column;gap:8px;background:var(--%NS%mat-sys-surface-container-low)}.rating-form[_ngcontent-%COMP%]   .full-width[_ngcontent-%COMP%]{width:100%}.rating-form-actions[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px}.rate-button[_ngcontent-%COMP%]{background-color:#f2b2332e;border-color:#f2b23399;color:#000000de}.remove-button[_ngcontent-%COMP%]{background-color:#c0392b1f;border-color:#c0392b80;color:#000000de}.reservation-actions[_ngcontent-%COMP%]{margin-top:8px;display:flex;gap:8px;flex-wrap:wrap}.status-rezervisano[_ngcontent-%COMP%]{--%NS%mat-chip-elevated-container-color: var(--%NS%mat-sys-tertiary-container);--%NS%mat-chip-label-text-color: var(--%NS%mat-sys-on-tertiary-container)}.status-pristiglo[_ngcontent-%COMP%]{--%NS%mat-chip-elevated-container-color: var(--%NS%mat-sys-primary-container);--%NS%mat-chip-label-text-color: var(--%NS%mat-sys-on-primary-container)}.status-otkazano[_ngcontent-%COMP%]{--%NS%mat-chip-elevated-container-color: var(--%NS%mat-sys-surface-variant);--%NS%mat-chip-label-text-color: var(--%NS%mat-sys-on-surface-variant)}.cart-summary[_ngcontent-%COMP%]{display:flex;justify-content:flex-end;gap:8px;margin-top:24px;padding-top:16px;border-top:1px solid var(--%NS%mat-sys-outline-variant);font-size:18px}.total-price[_ngcontent-%COMP%]{font-weight:700}.reservation-cancelled[_ngcontent-%COMP%]{opacity:.6}"]})};function I3(t,n){if(t&1){let e=Ie();m(0,"div",1)(1,"button",2),I("click",function(){q(e);let r=D();return Y(r.action())}),v(2),h()()}if(t&2){let e=D();g(2),pe(" ",e.data.action," ")}}var N3=["label"];function T3(t,n){}var k3=Math.pow(2,31)-1,Qd=class{_overlayRef;instance;containerInstance;_afterDismissed=new N;_afterOpened=new N;_onAction=new N;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,k3))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},aT=new w("MatSnackBarData"),Sc=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},A3=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),R3=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),O3=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),P3=(()=>{class t{snackBarRef=u(Qd);data=u(aT);action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(m(0,"div",0),v(1),h(),R(2,I3,3,1,"div",1)),i&2&&(g(),pe(" ",r.data.message,`
`),g(),O(r.hasAction?2:-1))},dependencies:[St,A3,R3,O3],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2})}return t})(),e0="_mat-snack-bar-enter",t0="_mat-snack-bar-exit",F3=(()=>{class t extends hc{_ngZone=u(H);_elementRef=u(U);_changeDetectorRef=u(De);_platform=u(xe);_animationsDisabled=We();snackBarConfig=u(Sc);_document=u($);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=u(te);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new N;_onExit=new N;_onEnter=new N;_animationState="void";_live;_label;_role;_liveElementId=u(Ye).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===t0?this._completeExit():e===e0&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?mt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(e0)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(e0)},200)))}exit(){return this._destroyed?X(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?mt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(t0)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(t0),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(a=>e.classList.add(a)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],a=o.getAttribute("aria-owns");this._trackedModals.add(o),a?a.indexOf(e)===-1&&o.setAttribute("aria-owns",a+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&nt(mc,7)(N3,7),i&2){let o;K(o=Z())&&(r._portalOutlet=o.first),K(o=Z())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&I("animationend",function(a){return r.onAnimationEnd(a.animationName)})("animationcancel",function(a){return r.onAnimationEnd(a.animationName)}),i&2&&z("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[Be],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(m(0,"div",1)(1,"div",2,0)(3,"div",3),jn(4,T3,0,0,"ng-template",4),h(),de(5,"div"),h()()),i&2&&(g(5),ee("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[mc],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--%NS%mat-snack-bar-supporting-text-color, var(--%NS%mat-sys-inverse-on-surface));
  border-radius: var(--%NS%mat-snack-bar-container-shape, var(--%NS%mat-sys-corner-extra-small));
  background-color: var(--%NS%mat-snack-bar-container-color, var(--%NS%mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--%NS%mat-snack-bar-supporting-text-font, var(--%NS%mat-sys-body-medium-font));
  font-size: var(--%NS%mat-snack-bar-supporting-text-size, var(--%NS%mat-sys-body-medium-size));
  font-weight: var(--%NS%mat-snack-bar-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight));
  line-height: var(--%NS%mat-snack-bar-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--%NS%mat-snack-bar-button-color, var(--%NS%mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --%NS%mat-button-text-state-layer-color: currentColor;
  --%NS%mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2,changeDetection:1})}return t})(),L3=new w("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Sc}),sT=(()=>{class t{_live=u(md);_injector=u(te);_breakpointObserver=u(hd);_parentSnackBar=u(t,{optional:!0,skipSelf:!0});_defaultConfig=u(L3);_animationsDisabled=We();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=P3;snackBarContainerComponent=F3;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=C(C({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=te.create({parent:r||this._injector,providers:[{provide:Sc,useValue:i}]}),a=new Oi(this.snackBarContainerComponent,i.viewContainerRef,o),s=e.attach(a);return s.instance.snackBarConfig=i,s.instance}_attach(e,i){let r=C(C(C({},new Sc),this._defaultConfig),i),o=this._createOverlay(r),a=this._attachSnackBarContainer(o,r),s=new Qd(a,o);if(e instanceof un){let c=new dr(e,null,{$implicit:r.data,snackBarRef:s});s.instance=a.attachTemplatePortal(c)}else{let c=this._createInjector(r,s),l=new Oi(e,void 0,c),d=a.attachComponentPortal(l);s.instance=d.instance}return this._breakpointObserver.observe(oM.HandsetPortrait).pipe(Ee(o.detachments())).subscribe(c=>{o.overlayElement.classList.toggle(this.handsetCssClass,c.matches)}),r.announcementMessage&&a._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(s,r),this._openedSnackBarRef=s,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Pi;i.direction=e.direction;let r=_c(this._injector),o=e.direction==="rtl",a=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,s=!a&&e.horizontalPosition!=="center";return a?r.left("0"):s?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Li(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return te.create({parent:r||this._injector,providers:[{provide:Qd,useValue:i},{provide:aT,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();var j3=(t,n)=>n.reviewId;function B3(t,n){t&1&&(m(0,"p"),v(1,"U\u010Ditavanje..."),h())}function U3(t,n){if(t&1&&(m(0,"p",1),v(1),h()),t&2){let e=D();g(),G(e.error())}}function H3(t,n){if(t&1&&(m(0,"mat-icon"),v(1),h()),t&2){let e=n.$implicit,i=D(2);g(),G(i.averageStarIcon(e))}}function z3(t,n){if(t&1&&(m(0,"span"),v(1),h(),m(2,"span",8),v(3),h()),t&2){let e=D(2);g(),pe("",n," / 5"),g(2),pe("(",e.reviews().length," recenzija)")}}function $3(t,n){t&1&&(m(0,"span",8),v(1,"Jo\u0161 uvek nema recenzija"),h())}function G3(t,n){if(t&1&&(m(0,"p",14),v(1),h()),t&2){let e=D(2);g(),G(e.reservationMessage())}}function W3(t,n){if(t&1&&(m(0,"p",1),v(1),h()),t&2){let e=D(2);g(),G(e.reservationError())}}function q3(t,n){t&1&&(m(0,"p",19),v(1,"Jo\u0161 uvek nema recenzija za ovu igra\u010Dku."),h())}function Y3(t,n){if(t&1&&(m(0,"mat-icon"),v(1),h()),t&2){let e=n.$implicit,i=D().$implicit;g(),G(i.rating>=e?"star":"star_border")}}function K3(t,n){if(t&1&&(m(0,"mat-card",21)(1,"div",22)(2,"span",23),v(3),m(4,"span",24),v(5),h()(),m(6,"span",25),at(7,Y3,2,1,"mat-icon",null,_a),h()(),m(9,"p",26),v(10),h(),m(11,"p",27),v(12),ba(13,"date"),h()()),t&2){let e=n.$implicit,i=D(2);g(3),pe(" ",e.authorName," "),g(2),pe("(",e.authorType,")"),g(2),st(i.stars),g(3),G(e.comment),g(2),G(Ca(13,4,e.date,"dd.MM.yyyy."))}}function Z3(t,n){if(t&1){let e=Ie();m(0,"a",2)(1,"mat-icon"),v(2,"arrow_back"),h(),v(3," Nazad na igra\u010Dke "),h(),m(4,"div",3)(5,"div",4),de(6,"img",5),h(),m(7,"div",6)(8,"h1"),v(9),h(),m(10,"div",7),at(11,H3,2,1,"mat-icon",null,_a),R(13,z3,4,2)(14,$3,2,0,"span",8),h(),m(15,"p",9),v(16),h(),m(17,"div",10)(18,"div",11)(19,"span",12),v(20,"Tip:"),h(),m(21,"span"),v(22),h()(),m(23,"div",11)(24,"span",12),v(25,"Uzrast:"),h(),m(26,"span"),v(27),h()(),m(28,"div",11)(29,"span",12),v(30,"Ciljna grupa:"),h(),m(31,"span"),v(32),h()(),m(33,"div",11)(34,"span",12),v(35,"Datum proizvodnje:"),h(),m(36,"span"),v(37),ba(38,"date"),h()()(),m(39,"p",13),v(40),h(),R(41,G3,2,1,"p",14),R(42,W3,2,1,"p",1),m(43,"button",15),I("click",function(){q(e);let r=D();return Y(r.onReserve())}),m(44,"mat-icon"),v(45,"shopping_cart"),h(),v(46," Rezervi\u0161i "),h()()(),m(47,"section",16)(48,"h2"),v(49,"Recenzije"),h(),m(50,"p",17),v(51," Recenziju je mogu\u0107e ostaviti isklju\u010Divo za igra\u010Dke koje ste rezervisali i koje imaju status "),m(52,"strong"),v(53,"pristiglo"),h(),v(54," - to se radi na stranici "),m(55,"a",18),v(56,"Korpa"),h(),v(57,". "),h(),R(58,q3,2,0,"p",19),m(59,"div",20),at(60,K3,14,7,"mat-card",21,j3),h()()}if(t&2){let e,i=n,r=D();g(6),T("src",r.imageBaseUrl+i.imageUrl,tr)("alt",i.name),g(3),G(i.name),g(2),st(r.stars),g(2),O((e=r.averageRating())?13:14,e),g(3),G(i.description),g(6),G(i.type.name),g(5),G(i.ageGroup.name),g(5),G(r.targetGroupLabel(i.targetGroup)),g(5),G(Ca(38,13,i.productionDate,"dd.MM.yyyy.")),g(3),pe("",i.price," RSD"),g(),O(r.reservationMessage()?41:-1),g(),O(r.reservationError()?42:-1),g(16),O(r.reviews().length===0?58:-1),g(2),st(r.reviews())}}var ap=class t{constructor(n,e,i){this.route=n;this.router=e;this.snackBar=i}route;router;snackBar;imageBaseUrl="https://toy.pequla.com";stars=[1,2,3,4,5];targetGroupLabel=Xm;toy=S(null);loading=S(!0);error=S(null);reviews=S([]);averageRating=S(null);averageRatingRounded=ct(()=>{let n=this.averageRating();return n==null?null:Math.round(n*2)/2});reservationMessage=S(null);reservationError=S(null);get isLoggedIn(){return Ke.getCurrentUser()!==null}ngOnInit(){let n=this.route.snapshot.paramMap.get("permalink");if(!n){this.error.set("Igra\u010Dka nije prona\u0111ena."),this.loading.set(!1);return}this.loadToy(n)}async loadToy(n){this.loading.set(!0),this.error.set(null);try{let e=await pn.getToyByPermalink(n);this.toy.set(e.data),this.loadReviews()}catch{this.error.set("Ne mogu da u\u010Ditam igra\u010Dku. Poku\u0161ajte ponovo.")}finally{this.loading.set(!1)}}loadReviews(){let n=this.toy();n&&(this.reviews.set(zr.getReviewsForToy(n.toyId)),this.averageRating.set(zr.getAverageRating(n.toyId)))}averageStarIcon(n){let e=this.averageRatingRounded();return e==null?"star_border":e>=n?"star":e>=n-.5?"star_half":"star_border"}onReserve(){if(this.reservationError.set(null),this.reservationMessage.set(null),!this.isLoggedIn){this.router.navigate(["/login"],{queryParams:{returnUrl:this.router.url}});return}let n=this.toy();if(n)try{let{merged:e}=Ke.createReservation(n);e?(this.reservationMessage.set("Igra\u010Dka je ve\u0107 u va\u0161oj korpi \u2014 koli\u010Dina je pove\u0107ana."),this.snackBar.open("Koli\u010Dina u korpi je pove\u0107ana.","Zatvori",{duration:3e3})):(this.reservationMessage.set("Igra\u010Dka je rezervisana i dodata u va\u0161u korpu."),this.snackBar.open("Igra\u010Dka je dodata u korpu.","Zatvori",{duration:3e3}))}catch(e){this.reservationError.set(e instanceof Error?e.message:"Rezervacija nije uspela.")}}static \u0275fac=function(e){return new(e||t)(ie(mn),ie(qt),ie(sT))};static \u0275cmp=F({type:t,selectors:[["app-details"]],decls:4,vars:3,consts:[[1,"details-page"],[1,"error"],["routerLink","/igracke",1,"back-link"],[1,"details-layout"],[1,"toy-image-col"],[1,"toy-image",3,"src","alt"],[1,"toy-info-col"],[1,"average-rating"],[1,"review-count"],[1,"description"],[1,"meta-grid"],[1,"meta-item"],[1,"meta-label"],[1,"price"],[1,"success"],["mat-raised-button","","color","primary",1,"reserve-button",3,"click"],[1,"reviews-section"],[1,"reviews-hint"],["routerLink","/cart"],[1,"no-reviews"],[1,"review-list"],[1,"review-card"],[1,"review-header"],[1,"review-author"],[1,"review-author-type"],[1,"review-stars"],[1,"review-comment"],[1,"review-date"]],template:function(e,i){if(e&1&&(m(0,"div",0),R(1,B3,2,0,"p"),R(2,U3,2,1,"p",1),R(3,Z3,62,16),h()),e&2){let r;g(),O(i.loading()?1:-1),g(),O(i.error()?2:-1),g(),O((r=!i.loading()&&!i.error()&&i.toy())?3:-1,r)}},dependencies:[Ht,xn,En,Vt,St,In,Mn,jl],styles:[".details-page[_ngcontent-%COMP%]{max-width:1000px;margin:0 auto;padding:24px}.details-layout[_ngcontent-%COMP%]{display:flex;gap:32px;align-items:flex-start}.toy-image-col[_ngcontent-%COMP%]{flex:0 0 360px}.toy-image[_ngcontent-%COMP%]{width:100%;border-radius:12px;object-fit:cover}.toy-info-col[_ngcontent-%COMP%]{flex:1}.back-link[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:4px;margin-bottom:16px;padding:6px 12px;border-radius:8px;background-color:var(--%NS%mat-sys-secondary-container);color:#000000de;font-size:14px;text-decoration:none}.back-link[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:18px;width:18px;height:18px}.toy-info-col[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:0 0 8px}.average-rating[_ngcontent-%COMP%]{display:flex;align-items:center;gap:4px;color:var(--%NS%mat-sys-tertiary);margin-bottom:12px}.average-rating[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-tertiary)}.review-count[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-surface-variant);font-size:13px}.description[_ngcontent-%COMP%]{margin:0 0 16px;line-height:1.5}.meta-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(2,1fr);gap:12px 24px;margin-bottom:16px}.meta-item[_ngcontent-%COMP%]{display:flex;flex-direction:column}.meta-label[_ngcontent-%COMP%]{font-size:12px;color:var(--%NS%mat-sys-on-surface-variant)}.price[_ngcontent-%COMP%]{font-size:24px;font-weight:700;margin:0 0 16px}.reserve-button[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:8px}.success[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-primary);font-size:14px}.error[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-error);font-size:14px}.reviews-section[_ngcontent-%COMP%]{margin-top:48px}.no-reviews[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-surface-variant)}.review-list[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:12px;margin-bottom:24px}.review-card[_ngcontent-%COMP%]{padding:16px}.review-header[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}.review-author[_ngcontent-%COMP%]{font-weight:600}.review-author-type[_ngcontent-%COMP%]{font-weight:400;color:var(--%NS%mat-sys-on-surface-variant);font-size:13px}.review-stars[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-tertiary);font-size:18px;height:18px;width:18px;vertical-align:middle}.review-comment[_ngcontent-%COMP%]{margin:0 0 4px}.review-date[_ngcontent-%COMP%]{margin:0;font-size:12px;color:var(--%NS%mat-sys-on-surface-variant)}.add-review-card[_ngcontent-%COMP%]{padding:24px;max-width:480px}.add-review-card[_ngcontent-%COMP%]   h3[_ngcontent-%COMP%]{margin:0 0 16px}.full-width[_ngcontent-%COMP%]{width:100%}.rating-picker[_ngcontent-%COMP%]{display:flex;align-items:center;gap:4px;margin-bottom:16px}.rating-picker[_ngcontent-%COMP%]   .star[_ngcontent-%COMP%]{cursor:pointer;color:var(--%NS%mat-sys-tertiary)}.login-prompt[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-surface-variant)}@media(max-width:720px){.details-layout[_ngcontent-%COMP%]{flex-direction:column}.toy-image-col[_ngcontent-%COMP%]{flex:none;width:100%}.meta-grid[_ngcontent-%COMP%]{grid-template-columns:1fr}}"]})};var n0=()=>{let t=u(qt);return Ke.getCurrentUser()?!0:(t.navigateByUrl("/login"),!1)};var cT=[{path:"",title:"ToyBox",component:Cm},{path:"igracke",title:"Igra\u010Dke",component:Jm},{path:"igracke/:permalink",title:"Detalji igra\u010Dke",component:ap},{path:"login",title:"Prijava",component:ep},{path:"signup",title:"Registracija",component:ip},{path:"profil",title:"Profil",component:rp,canActivate:[n0]},{path:"cart",title:"Korpa",component:op,canActivate:[n0]},{path:"**",redirectTo:""}];var lT={providers:[kg(),my(cT)]};var Q3=["*",[["mat-toolbar-row"]]],X3=["*","mat-toolbar-row"],J3=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),dT=(()=>{class t{_elementRef=u(U);_platform=u(xe);_document=u($);color;_toolbarRows;ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&Wt(o,J3,5),i&2){let a;K(a=Z())&&(r._toolbarRows=a)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(bt(r.color?"mat-"+r.color:""),z("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:X3,decls:2,vars:0,template:function(i,r){i&1&&(ke(Q3),se(0),se(1,1))},styles:[`.mat-toolbar {
  background: var(--%NS%mat-toolbar-container-background-color, var(--%NS%mat-sys-surface));
  color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--%NS%mat-toolbar-title-text-font, var(--%NS%mat-sys-title-large-font));
  font-size: var(--%NS%mat-toolbar-title-text-size, var(--%NS%mat-sys-title-large-size));
  line-height: var(--%NS%mat-toolbar-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-weight: var(--%NS%mat-toolbar-title-text-weight, var(--%NS%mat-sys-title-large-weight));
  letter-spacing: var(--%NS%mat-toolbar-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --%NS%mat-button-text-label-text-color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
  --%NS%mat-button-outlined-label-text-color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--%NS%mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--%NS%mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--%NS%mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--%NS%mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2})}return t})();var uT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Ne]})}return t})();var t4=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],n4=["mat-icon, [matMenuItemIcon]","*"];function i4(t,n){t&1&&(yn(),m(0,"svg",2),de(1,"polygon",3),h())}var r4=["*"];function o4(t,n){if(t&1){let e=Ie();Ve(0,"div",0),ho("click",function(){q(e);let r=D();return Y(r.closed.emit("click"))})("animationstart",function(r){q(e);let o=D();return Y(o._onAnimationStart(r.animationName))})("animationend",function(r){q(e);let o=D();return Y(o._onAnimationDone(r.animationName))})("animationcancel",function(r){q(e);let o=D();return Y(o._onAnimationDone(r.animationName))}),Ve(1,"div",1),se(2),$e()()}if(t&2){let e=D();bt(e._classList),z("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),pt("id",e.panelId),ee("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var r0=new w("MAT_MENU_PANEL"),Xd=(()=>{class t{_elementRef=u(U);_document=u($);_focusMonitor=u(Ii);_parentMenu=u(r0,{optional:!0});_changeDetectorRef=u(De);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new N;_focused=new N;_highlighted=!1;_triggersSubmenu=!1;constructor(){u(Ge).load($n),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&I("click",function(a){return r._checkDisabled(a)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(ee("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),z("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",j],disableRipple:[2,"disableRipple","disableRipple",j]},exportAs:["matMenuItem"],ngContentSelectors:n4,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(ke(t4),se(0),m(1,"span",0),se(2,1),h(),de(3,"div",1),R(4,i4,2,0,":svg:svg",2)),i&2&&(g(3),T("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),g(),O(r._triggersSubmenu?4:-1))},dependencies:[Ks],encapsulation:2})}return t})();var a4=new w("MatMenuContent");var s4=new w("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),i0="_mat-menu-enter",sp="_mat-menu-exit",Ec=(()=>{class t{_elementRef=u(U);_changeDetectorRef=u(De);_injector=u(te);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=We();_allItems;_directDescendantItems=new Si;_classList={};_panelAnimationState="void";_animationDone=new N;_isAnimating=S(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;get panelClass(){return this._previousPanelClass}set panelClass(e){let i=this._previousPanelClass,r=C({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass="";get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new A;close=this.closed;panelId=u(Ye).getId("mat-menu-panel-");constructor(){let e=u(s4);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new Pa(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(dt(this._directDescendantItems),ht(e=>Rt(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(dt(this._directDescendantItems),ht(i=>Rt(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:Ct(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=mt(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=re(C({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===sp;(i||e===i0)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===i0||e===sp)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(sp),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?i0:sp)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(dt(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&Wt(o,a4,5)(o,Xd,5)(o,Xd,4),i&2){let a;K(a=Z())&&(r.lazyContent=a.first),K(a=Z())&&(r._allItems=a),K(a=Z())&&(r.items=a)}},viewQuery:function(i,r){if(i&1&&nt(un,5),i&2){let o;K(o=Z())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&ee("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",j],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:j(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[Re([{provide:r0,useExisting:t}])],ngContentSelectors:r4,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(ke(),Il(0,o4,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--%NS%mat-menu-item-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-menu-item-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-menu-item-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-menu-item-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  font-weight: var(--%NS%mat-menu-item-label-text-weight, var(--%NS%mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--%NS%mat-menu-container-shape, var(--%NS%mat-sys-corner-extra-small));
  background-color: var(--%NS%mat-menu-container-color, var(--%NS%mat-sys-surface-container));
  box-shadow: var(--%NS%mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--%NS%mat-menu-divider-color, var(--%NS%mat-sys-surface-variant));
  margin-bottom: var(--%NS%mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--%NS%mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--%NS%mat-menu-item-leading-spacing, 12px);
  padding-right: var(--%NS%mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--%NS%mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--%NS%mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--%NS%mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--%NS%mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--%NS%mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--%NS%mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--%NS%mat-menu-item-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--%NS%mat-menu-item-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--%NS%mat-menu-item-spacing, 12px);
  height: var(--%NS%mat-menu-item-icon-size, 24px);
  width: var(--%NS%mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--%NS%mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--%NS%mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--%NS%mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--%NS%mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--%NS%mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--%NS%mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2})}return t})(),c4=new w("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(te);return()=>Fi(t)}});var Dc=new WeakMap,l4=(()=>{class t{_canHaveBackdrop;_element=u(U);_viewContainerRef=u(Lt);_menuItemInstance=u(Xd,{optional:!0,self:!0});_dir=u(Tt,{optional:!0});_focusMonitor=u(Ii);_ngZone=u(H);_injector=u(te);_scrollStrategy=u(c4);_changeDetectorRef=u(De);_animationsDisabled=We();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=ue.EMPTY;_menuCloseSubscription=ue.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e?(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})):this._destroyMenu(),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=u(r0,{optional:!0});this._parentMaterialMenu=i instanceof Ec?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Dc.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Dc.get(i);Dc.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),a=o.getConfig(),s=a.positionStrategy;this._setPosition(i,s),this._canHaveBackdrop?a.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:a.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof Ec&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(Ee(i.close)).subscribe(()=>{s.withLockedPosition(!1).reapplyLastPosition(),s.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof Ec&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(Ot(1)).subscribe(()=>{i.detach(),Dc.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Dc.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=Li(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof Ec&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new Pi({positionStrategy:Hr(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",a=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,a)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[a,s]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[c,l]=[a,s],[d,f]=[r,o],p=0;if(this._triggersSubmenu()){if(f=r=e.xPosition==="before"?"start":"end",o=d=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let _=this._parentMaterialMenu.items.first;this._parentInnerPadding=_?_._getHostElement().offsetTop:0}p=a==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(c=a==="top"?"bottom":"top",l=s==="top"?"bottom":"top");i.withPositions([{originX:r,originY:c,overlayX:d,overlayY:a,offsetY:p},{originX:o,originY:c,overlayX:f,overlayY:a,offsetY:p},{originX:r,originY:l,overlayX:d,overlayY:s,offsetY:-p},{originX:o,originY:l,overlayX:f,overlayY:s,offsetY:-p}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:X(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(je(a=>this._menuOpen&&a!==this._menuItemInstance)):X();return Rt(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new dr(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Dc.get(e)===this}_triggerIsAriaDisabled(){return j(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){ga()};static \u0275dir=P({type:t})}return t})(),fT=(()=>{class t extends l4{_cleanupTouchstart;_hoverSubscription=ue.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new A;onMenuOpen=this.menuOpened;menuClosed=new A;onMenuClose=this.menuClosed;constructor(){super(!0);let e=u(Te);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{Aa(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){ka(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&I("click",function(a){return r._handleClick(a)})("mousedown",function(a){return r._handleMousedown(a)})("keydown",function(a){return r._handleKeydown(a)}),i&2&&ee("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu?.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[Be]})}return t})();var hT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[Co,No,Ne,jr]})}return t})();var u4=new w("MAT_BADGE_CONFIG"),mT="mat-badge-content",f4=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=F({type:t,selectors:[["ng-component"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-badge {
  position: relative;
}
.mat-badge.mat-badge {
  overflow: visible;
}

.mat-badge-content {
  position: absolute;
  text-align: center;
  display: inline-block;
  transition: transform 200ms ease-in-out;
  transform: scale(0.6);
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
  box-sizing: border-box;
  pointer-events: none;
  background-color: var(--%NS%mat-badge-background-color, var(--%NS%mat-sys-error));
  color: var(--%NS%mat-badge-text-color, var(--%NS%mat-sys-on-error));
  font-family: var(--%NS%mat-badge-text-font, var(--%NS%mat-sys-label-small-font));
  font-weight: var(--%NS%mat-badge-text-weight, var(--%NS%mat-sys-label-small-weight));
  border-radius: var(--%NS%mat-badge-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-badge-above .mat-badge-content {
  bottom: 100%;
}
.mat-badge-below .mat-badge-content {
  top: 100%;
}
.mat-badge-before .mat-badge-content {
  right: 100%;
}
[dir=rtl] .mat-badge-before .mat-badge-content {
  right: auto;
  left: 100%;
}
.mat-badge-after .mat-badge-content {
  left: 100%;
}
[dir=rtl] .mat-badge-after .mat-badge-content {
  left: auto;
  right: 100%;
}
@media (forced-colors: active) {
  .mat-badge-content {
    outline: solid 1px;
    border-radius: 0;
  }
}

.mat-badge-disabled .mat-badge-content {
  background-color: var(--%NS%mat-badge-disabled-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-error) 38%, transparent));
  color: var(--%NS%mat-badge-disabled-state-text-color, var(--%NS%mat-sys-on-error));
}

.mat-badge-hidden .mat-badge-content {
  display: none;
}

.ng-animate-disabled .mat-badge-content,
.mat-badge-content._mat-animation-noopable {
  transition: none;
}

.mat-badge-content.mat-badge-active {
  transform: none;
}

.mat-badge-small .mat-badge-content {
  width: var(--%NS%mat-badge-legacy-small-size-container-size, unset);
  height: var(--%NS%mat-badge-legacy-small-size-container-size, unset);
  min-width: var(--%NS%mat-badge-small-size-container-size, 6px);
  min-height: var(--%NS%mat-badge-small-size-container-size, 6px);
  line-height: var(--%NS%mat-badge-small-size-line-height, 6px);
  padding: var(--%NS%mat-badge-small-size-container-padding, 0);
  font-size: var(--%NS%mat-badge-small-size-text-size, 0);
  margin: var(--%NS%mat-badge-small-size-container-offset, -6px 0);
}
.mat-badge-small.mat-badge-overlap .mat-badge-content {
  margin: var(--%NS%mat-badge-small-size-container-overlap-offset, -6px);
}

.mat-badge-medium .mat-badge-content {
  width: var(--%NS%mat-badge-legacy-container-size, unset);
  height: var(--%NS%mat-badge-legacy-container-size, unset);
  min-width: var(--%NS%mat-badge-container-size, 16px);
  min-height: var(--%NS%mat-badge-container-size, 16px);
  line-height: var(--%NS%mat-badge-line-height, 16px);
  padding: var(--%NS%mat-badge-container-padding, 0 4px);
  font-size: var(--%NS%mat-badge-text-size, var(--%NS%mat-sys-label-small-size));
  margin: var(--%NS%mat-badge-container-offset, -12px 0);
}
.mat-badge-medium.mat-badge-overlap .mat-badge-content {
  margin: var(--%NS%mat-badge-container-overlap-offset, -12px);
}

.mat-badge-large .mat-badge-content {
  width: var(--%NS%mat-badge-legacy-large-size-container-size, unset);
  height: var(--%NS%mat-badge-legacy-large-size-container-size, unset);
  min-width: var(--%NS%mat-badge-large-size-container-size, 16px);
  min-height: var(--%NS%mat-badge-large-size-container-size, 16px);
  line-height: var(--%NS%mat-badge-large-size-line-height, 16px);
  padding: var(--%NS%mat-badge-large-size-container-padding, 0 4px);
  font-size: var(--%NS%mat-badge-large-size-text-size, var(--%NS%mat-sys-label-small-size));
  margin: var(--%NS%mat-badge-large-size-container-offset, -12px 0);
}
.mat-badge-large.mat-badge-overlap .mat-badge-content {
  margin: var(--%NS%mat-badge-large-size-container-overlap-offset, -12px);
}
`],encapsulation:2})}return t})(),pT=(()=>{class t{_ngZone=u(H);_elementRef=u(U);_ariaDescriber=u(im);_renderer=u(Te);_animationsDisabled=We();_idGenerator=u(Ye);get color(){return this._color}set color(e){this._setColor(e),this._color=e}_color;overlap;disabled=!1;position;get content(){return this._content}set content(e){this._updateRenderedContent(e)}_content;get description(){return this._description}set description(e){this._updateDescription(e)}_description;size;hidden=!1;_badgeElement;_inlineBadgeDescription;_isInitialized=!1;_interactivityChecker=u(Qh);_document=u($);constructor(){let e=u(u4,{optional:!0}),i=u(Ge);i.load(f4),i.load(Dn),this._color=e?.color||"primary",this.overlap=e?.overlap??!0,this.position=e?.position||"above after",this.size=e?.size||"medium"}isAbove(){return this.position.indexOf("below")===-1}isAfter(){return this.position.indexOf("before")===-1}getBadgeElement(){return this._badgeElement}ngOnInit(){this._clearExistingBadges(),this.content&&!this._badgeElement&&(this._badgeElement=this._createBadgeElement(),this._updateRenderedContent(this.content)),this._isInitialized=!0}ngAfterViewInit(){}ngOnDestroy(){this._renderer.destroyNode&&(this._renderer.destroyNode(this._badgeElement),this._inlineBadgeDescription?.remove()),this._ariaDescriber.removeDescription(this._elementRef.nativeElement,this.description)}_isHostInteractive(){return this._interactivityChecker.isFocusable(this._elementRef.nativeElement,{ignoreVisibility:!0})}_createBadgeElement(){let e=this._renderer.createElement("span"),i="mat-badge-active";return e.setAttribute("id",this._idGenerator.getId("mat-badge-content-")),e.setAttribute("aria-hidden","true"),e.classList.add(mT),this._animationsDisabled&&e.classList.add("_mat-animation-noopable"),this._elementRef.nativeElement.appendChild(e),typeof requestAnimationFrame=="function"&&!this._animationsDisabled?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>{e.classList.add(i)})}):e.classList.add(i),e}_updateRenderedContent(e){let i=`${e??""}`.trim();this._isInitialized&&i&&!this._badgeElement&&(this._badgeElement=this._createBadgeElement()),this._badgeElement&&(this._badgeElement.textContent=i),this._content=i}_updateDescription(e){this._ariaDescriber.removeDescription(this._elementRef.nativeElement,this.description),(!e||this._isHostInteractive())&&this._removeInlineDescription(),this._description=e,this._isHostInteractive()?this._ariaDescriber.describe(this._elementRef.nativeElement,e):this._updateInlineDescription()}_updateInlineDescription(){this._inlineBadgeDescription||(this._inlineBadgeDescription=this._document.createElement("span"),this._inlineBadgeDescription.classList.add("cdk-visually-hidden")),this._inlineBadgeDescription.textContent=this.description,this._badgeElement?.appendChild(this._inlineBadgeDescription)}_removeInlineDescription(){this._inlineBadgeDescription?.remove(),this._inlineBadgeDescription=void 0}_setColor(e){let i=this._elementRef.nativeElement.classList;i.remove(`mat-badge-${this._color}`),e&&i.add(`mat-badge-${e}`)}_clearExistingBadges(){let e=this._elementRef.nativeElement.querySelectorAll(`:scope > .${mT}`);for(let i of Array.from(e))i!==this._badgeElement&&i.remove()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matBadge",""]],hostAttrs:[1,"mat-badge"],hostVars:20,hostBindings:function(i,r){i&2&&z("mat-badge-overlap",r.overlap)("mat-badge-above",r.isAbove())("mat-badge-below",!r.isAbove())("mat-badge-before",!r.isAfter())("mat-badge-after",r.isAfter())("mat-badge-small",r.size==="small")("mat-badge-medium",r.size==="medium")("mat-badge-large",r.size==="large")("mat-badge-hidden",r.hidden||!r.content)("mat-badge-disabled",r.disabled)},inputs:{color:[0,"matBadgeColor","color"],overlap:[2,"matBadgeOverlap","overlap",j],disabled:[2,"matBadgeDisabled","disabled",j],position:[0,"matBadgePosition","position"],content:[0,"matBadge","content"],description:[0,"matBadgeDescription","description"],size:[0,"matBadgeSize","size"],hidden:[2,"matBadgeHidden","hidden",j]}})}return t})(),gT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=ne({imports:[pd,Ne]})}return t})();function m4(t,n){if(t&1&&(m(0,"span"),v(1),h()),t&2){let e=D();g(),pe("(",e.cartCount(),")")}}function p4(t,n){if(t&1){let e=Ie();m(0,"button",15),I("click",function(){q(e);let r=D();return Y(r.clearSearch())}),v(1," \xD7 "),h()}}function g4(t,n){if(t&1){let e=Ie();m(0,"button",16)(1,"span",17),v(2),h(),m(3,"mat-icon"),v(4,"account_circle"),h(),m(5,"mat-icon",18),v(6,"arrow_drop_down"),h()(),m(7,"mat-menu",null,0)(9,"a",19)(10,"mat-icon"),v(11,"person"),h(),m(12,"span"),v(13,"Profil"),h()(),m(14,"button",20),I("click",function(){q(e);let r=D();return Y(r.logout())}),m(15,"mat-icon"),v(16,"logout"),h(),m(17,"span"),v(18,"Odjava"),h()()()}if(t&2){let e=ii(8),i=D();T("matMenuTriggerFor",e),g(2),G(i.currentUser().firstName)}}function _4(t,n){t&1&&(m(0,"div",12)(1,"mat-icon"),v(2,"person"),h(),m(3,"a",21),v(4,"Prijava"),h(),m(5,"span",22),v(6,"/"),h(),m(7,"a",23),v(8,"Registracija"),h()())}var cp=class t{constructor(n){this.router=n}router;title=S("pki-ispit-2026");currentUser=Ke.currentUser;cartCount=Ke.activeCartCount;searchTerm=Kt.searchTerm;logout(){Ke.logout(),Kt.clear(),this.router.navigateByUrl("/")}onSearch(){this.router.navigateByUrl("/igracke")}clearSearch(){Kt.searchTerm.set("")}static \u0275fac=function(e){return new(e||t)(ie(qt))};static \u0275cmp=F({type:t,selectors:[["app-root"]],decls:28,vars:6,consts:[["userMenu","matMenu"],["color","primary"],[1,"toolbar-inner"],["routerLink","/",1,"brand",3,"click"],["src","toybox-logo.png","alt","ToyBox logo",1,"brand-logo"],["mat-button","","routerLink","/igracke","routerLinkActive","active-link",3,"click"],["mat-button","","routerLink","/cart","routerLinkActive","active-link",3,"click"],["matBadgeSize","small","matBadgeColor","accent",3,"matBadge","matBadgeHidden"],[1,"toolbar-search"],[3,"click"],["type","text","placeholder","Pretra\u017Ei igra\u010Dke...","aria-label","Pretra\u017Ei igra\u010Dke",3,"ngModelChange","keyup.enter","ngModel"],["type","button","aria-label","Obri\u0161i pretragu",1,"clear-search-btn"],[1,"auth-links"],[1,"site-footer"],[1,"footer-inner"],["type","button","aria-label","Obri\u0161i pretragu",1,"clear-search-btn",3,"click"],["mat-button","",1,"user-menu-trigger",3,"matMenuTriggerFor"],[1,"user-name"],[1,"dropdown-caret"],["mat-menu-item","","routerLink","/profil"],["mat-menu-item","",3,"click"],["routerLink","/login"],[1,"auth-separator"],["routerLink","/signup"]],template:function(e,i){e&1&&(m(0,"mat-toolbar",1)(1,"div",2)(2,"a",3),I("click",function(){return i.clearSearch()}),de(3,"img",4),m(4,"span"),v(5,"ToyBox"),h()(),m(6,"nav")(7,"a",5),I("click",function(){return i.clearSearch()}),m(8,"mat-icon"),v(9,"toys"),h(),v(10," Igra\u010Dke "),h(),m(11,"a",6),I("click",function(){return i.clearSearch()}),m(12,"mat-icon",7),v(13,"shopping_cart"),h(),v(14," Korpa "),R(15,m4,2,1,"span"),h()(),m(16,"div",8)(17,"mat-icon",9),I("click",function(){return i.onSearch()}),v(18,"search"),h(),m(19,"input",10),I("ngModelChange",function(o){return i.searchTerm.set(o)})("keyup.enter",function(){return i.onSearch()}),h(),Fe(),R(20,p4,2,0,"button",11),h(),R(21,g4,19,2)(22,_4,9,0,"div",12),h()(),de(23,"router-outlet"),m(24,"footer",13)(25,"div",14)(26,"span"),v(27,"\xA9 2026 ToyBox"),h()()()),e&2&&(g(12),T("matBadge",i.cartCount())("matBadgeHidden",i.cartCount()===0),g(3),O(i.cartCount()>0?15:-1),g(4),T("ngModel",i.searchTerm()),Le(),g(),O(i.searchTerm()?20:-1),g(),O(i.currentUser()?21:22))},dependencies:[cd,Ht,hy,pi,Tn,mi,Gn,uT,dT,Vt,St,In,Mn,hT,Ec,Xd,fT,gT,pT],styles:["mat-toolbar[_ngcontent-%COMP%]{display:flex;background:var(--%NS%mat-sys-primary);color:var(--%NS%mat-sys-on-primary)}mat-toolbar[_ngcontent-%COMP%]   a[_ngcontent-%COMP%], mat-toolbar[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-primary)}.toolbar-inner[_ngcontent-%COMP%]{width:100%;max-width:1400px;margin:0 auto;padding:0 24px;display:flex;align-items:center;gap:8px}.brand[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;text-decoration:none;font-size:20px;font-weight:700;color:var(--%NS%mat-sys-on-primary);margin-right:24px}.brand-logo[_ngcontent-%COMP%]{height:32px;width:auto;display:block}nav[_ngcontent-%COMP%]{display:flex;align-items:center;gap:4px}.active-link[_ngcontent-%COMP%]{background-color:#ffffff26;border-radius:8px}.toolbar-search[_ngcontent-%COMP%]{margin-left:auto;display:flex;align-items:center;gap:8px;background:#ffffff26;border-radius:20px;padding:6px 16px;max-width:380px;width:100%;margin-right:20px}.toolbar-search[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{font-size:18px;height:18px;width:18px;color:var(--%NS%mat-sys-on-primary);opacity:.75}.toolbar-search[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]{border:none;background:transparent;outline:none;color:var(--%NS%mat-sys-on-primary);font-size:14px;width:100%}.toolbar-search[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]::placeholder{color:var(--%NS%mat-sys-on-primary);opacity:.6}.clear-search-btn[_ngcontent-%COMP%]{flex-shrink:0;width:18px;height:18px;padding:0;border:none;border-radius:50%;background:#ffffff40;color:var(--%NS%mat-sys-on-primary);font-size:14px;line-height:1;display:flex;align-items:center;justify-content:center;cursor:pointer}.clear-search-btn[_ngcontent-%COMP%]:hover{background:#fff6}.auth-links[_ngcontent-%COMP%]{display:flex;align-items:center;gap:6px;font-size:14px}.auth-links[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-primary);text-decoration:none}.auth-links[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover{text-decoration:underline}.auth-separator[_ngcontent-%COMP%]{color:var(--%NS%mat-sys-on-primary);opacity:.5}.user-menu-trigger[_ngcontent-%COMP%]{display:flex;align-items:center;padding:0 10px}.user-name[_ngcontent-%COMP%]{order:1;color:var(--%NS%mat-sys-on-primary);margin-right:10px}.user-menu-trigger[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]:not(.dropdown-caret){order:2;margin-right:2}.dropdown-caret[_ngcontent-%COMP%]{order:3;margin-left:-6px}[_nghost-%COMP%]{display:flex;flex-direction:column;min-height:100vh}router-outlet[_ngcontent-%COMP%] + *[_ngcontent-%COMP%]{flex:1;display:flex;flex-direction:column;min-height:0}.site-footer[_ngcontent-%COMP%]{background:var(--%NS%mat-sys-primary);color:var(--%NS%mat-sys-on-primary);margin-top:0}.footer-inner[_ngcontent-%COMP%]{max-width:1400px;margin:0 auto;padding:10px 24px;display:flex;justify-content:center;align-items:center;font-size:13px}"]})};Tv(cp,lT).catch(t=>console.error(t));
