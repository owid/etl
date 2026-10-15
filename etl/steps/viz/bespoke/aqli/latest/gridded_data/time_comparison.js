/* Independent time comparison, sharing the prototype's source grids and palette. */
(async () => {
  const el = id => document.getElementById(`time-${id}`);
  const sides = ['left', 'right'];
  const maps = {}, layers = {}, outlines = {}, tokens = {left:0,right:0};
  let country = null, syncing = false;
  let overlay = null, overlayRequest;
  const cityLayers = {}, boundaryLayers = {};
  const normalize = value => value.normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
  try {
    const response = await fetch('countries.json', {cache:'no-store'});
    if (!response.ok) throw Error('Country data could not load.');
    const metadata = await response.json();
    const entries = metadata.features.map(feature => ({label:feature.properties.name, feature}));
    const year = side => Number(el(`year-${side}`).value);
    function heading(side) {
      const title = el(`title-${side}`);
      const yearLabel=document.createElement('span');yearLabel.className='selected-country-name';yearLabel.textContent=year(side);
      if (!country) {title.replaceChildren('Local air pollution in ',yearLabel);return;}
      const name = document.createElement('span');name.className='selected-country-name';name.textContent=country.properties.name;
      title.replaceChildren('Local air pollution in ',name,' in ',yearLabel);
    }
    function render(side) {
      const token = ++tokens[side];
      heading(side);el(`output-${side}`).textContent=year(side);
      if (layers[side]) {maps[side].removeLayer(layers[side]);layers[side]=null;}
      el(`status-${side}`).replaceChildren();
      if (!country) return;
      el(`status-${side}`).textContent=`Loading ${year(side)}…`;
      let failed=false;
      const layer=L.tileLayer(`/${metadata.tile_path}/${country.properties.id}/${year(side)}/{z}/{x}/{y}.png`,{noWrap:true,maxNativeZoom:10,maxZoom:12,keepBuffer:1,updateWhenIdle:true,attribution:'SatPM V6.GL.03'});
      layer.on('tileerror',()=>{
        if(token!==tokens[side] || failed)return;failed=true;
        const retry=document.createElement('button');retry.textContent='Retry loading';retry.onclick=()=>render(side);
        el(`status-${side}`).replaceChildren('Could not load this map. ',retry);
      });
      layer.on('load',()=>{if(token===tokens[side]&&!failed)el(`status-${side}`).textContent='';});
      layers[side]=layer;layer.addTo(maps[side]);
    }
    function drawCities(side) {
      const group=cityLayers[side];if(!group)return;group.clearLayers();
      if(!overlay || !el('show-cities').checked)return;
      const map=maps[side],size=map.getSize(),boxes=[];
      for(const city of overlay.cities){
        const location=L.latLng(city.lat,city.lon);if(!map.getBounds().contains(location))continue;
        const point=map.latLngToContainerPoint(location),width=Math.min(175,city.name.length*6.6);
        const box=[point.x-5,point.y-12,point.x+width+15,point.y+14];
        if(box[2]>size.x-4||box[0]<4||box[1]<4||box[3]>size.y-4)continue;
        if(boxes.some(b=>box[0]<b[2]&&box[2]>b[0]&&box[1]<b[3]&&box[3]>b[1]))continue;
        boxes.push(box);
        const content=document.createElement('div'),dot=document.createElement('span'),label=document.createElement('span');
        dot.className='city-dot';label.className='city-label';label.textContent=city.name;content.append(dot,label);
        L.marker(location,{pane:'cityLabels',interactive:false,keyboard:false,icon:L.divIcon({className:'city-marker',html:content,iconSize:[6,6],iconAnchor:[3,3]})}).addTo(group);
        if(boxes.length>=45)break;
      }
    }
    function drawOverlays(){
      for(const side of sides){
        if(boundaryLayers[side]){maps[side].removeLayer(boundaryLayers[side]);boundaryLayers[side]=null;}
        if(overlay&&el('show-boundaries').checked)boundaryLayers[side]=L.geoJSON(overlay.boundaries,{pane:'adminBoundaries',interactive:false,style:{color:'#526373',weight:.75,opacity:.6,fill:false}}).addTo(maps[side]);
        drawCities(side);
      }
    }
    async function loadOverlays(){
      if(overlayRequest)overlayRequest.abort();
      const request=new AbortController();overlayRequest=request;
      const id=country.properties.id;overlay=null;drawOverlays();
      el('show-cities').disabled=false;el('show-boundaries').disabled=false;
      el('overlay-status').textContent='Loading labels and boundaries…';
      try{
        const response=await fetch(`overlays/${id}.json`,{signal:request.signal});
        if(!response.ok)throw Error('Overlay request failed');
        const payload=await response.json();
        if(request!==overlayRequest||country?.properties.id!==id)return;
        overlay=payload;drawOverlays();el('overlay-status').textContent='';
      }catch(error){
        if(error.name==='AbortError'||request!==overlayRequest)return;
        const retry=document.createElement('button');retry.textContent='Retry overlays';retry.onclick=loadOverlays;
        el('overlay-status').replaceChildren('Labels and boundaries could not load. ',retry);
      }
    }
    function choose(entry) {
      country=entry.feature;el('search').value=entry.label;el('notice').hidden=true;
      el('picker-slot').append(el('picker'));el('empty').hidden=true;
      el('reset').hidden=false;el('clear').hidden=false;
      syncing=true;
      for(const side of sides) {
        // Retire old tiles before moving the viewport, avoiding unnecessary requests.
        tokens[side]++;if(layers[side]){maps[side].removeLayer(layers[side]);layers[side]=null;}
        if(outlines[side])maps[side].removeLayer(outlines[side]);
        outlines[side]=L.geoJSON(country,{style:{color:'#445d6c',weight:1,fill:false},interactive:false}).addTo(maps[side]);
        if(entry.city)maps[side].setView([entry.city.lat,entry.city.lon],10,{animate:false});
        else maps[side].fitBounds(country.properties.bounds,{padding:[24,24],maxZoom:9,animate:false});
        render(side);
      }
      syncing=false;loadOverlays();
    }
    for(const side of sides) {
      const map=L.map(`time-map-${side}`,{minZoom:2,maxZoom:12,worldCopyJump:false}).setView([15,0],2);maps[side]=map;
      map.createPane('adminBoundaries');map.getPane('adminBoundaries').style.zIndex=450;map.getPane('adminBoundaries').style.pointerEvents='none';
      map.createPane('cityLabels');map.getPane('cityLabels').style.zIndex=650;map.getPane('cityLabels').style.pointerEvents='none';
      cityLayers[side]=L.layerGroup().addTo(map);
      map.on('moveend zoomend resize',()=>drawCities(side));
      map.on('moveend',()=>{
        if(syncing)return;syncing=true;
        const other=maps[side==='left'?'right':'left'];
        if(other)other.setView(map.getCenter(),map.getZoom(),{animate:false});
        syncing=false;
      });
      heading(side);
      el(`year-${side}`).disabled=false;
      let timer;
      el(`year-${side}`).addEventListener('input',()=>{
        heading(side);el(`output-${side}`).textContent=year(side);
        clearTimeout(timer);timer=setTimeout(()=>render(side),120);
      });
    }
    el('show-cities').addEventListener('change',()=>sides.forEach(drawCities));
    el('show-boundaries').addEventListener('change',drawOverlays);
    const populate=()=>{
      const query=normalize(el('search').value);el('options').replaceChildren();
      entries.filter(entry=>normalize(entry.label).includes(query)).slice(0,60).forEach(entry=>{
        const option=document.createElement('option');option.value=entry.label;option.textContent=entry.label;el('options').append(option);
      });
    };
    el('search').disabled=false;populate();
    el('search').addEventListener('input',()=>{
      const entry=entries.find(entry=>entry.label===el('search').value);
      if(entry)choose(entry);else populate();
    });
    el('reset').onclick=()=>{if(country)maps.left.fitBounds(country.properties.bounds,{padding:[24,24],maxZoom:9,animate:false});};
    el('clear').onclick=()=>{
      if(overlayRequest){overlayRequest.abort();overlayRequest=null;}
      overlay=null;drawOverlays();el('overlay-status').replaceChildren();
      for(const id of ['show-cities','show-boundaries']){el(id).disabled=true;el(id).checked=true;}
      country=null;el('search').value='';populate();el('notice').hidden=false;
      el('empty').append(el('picker'));el('empty').hidden=false;
      el('reset').hidden=true;el('clear').hidden=true;
      for(const side of sides){el(`year-${side}`).value=side==='left'?1998:2024;render(side);if(outlines[side]){maps[side].removeLayer(outlines[side]);outlines[side]=null;}}
      maps.left.setView([15,0],2,{animate:false});el('search').focus({preventScroll:true});
    };
    metadata.colors.forEach((color,index)=>{
      const bin=document.createElement('div');bin.className='bin';
      const swatch=document.createElement('div');swatch.className='swatch';swatch.style.background=color;
      const label=document.createElement('span');label.className='bin-label';label.textContent=`${index===0?0:metadata.brackets[index-1]}${index===metadata.colors.length-1?'+':''}`;
      bin.append(swatch,label);el('scale').append(bin);
    });
    try {
      const result=await fetch('cities.json');if(!result.ok)throw Error('City lookup unavailable.');
      const {cities}=await result.json();const counts=new Map();
      cities.forEach(city=>{const key=`${city.name}, ${city.country}`;counts.set(key,(counts.get(key)||0)+1);});
      cities.forEach(city=>{
        const key=`${city.name}, ${city.country}`;
        const label=counts.get(key)>1?`${key} (${city.lat.toFixed(3)}°, ${city.lon.toFixed(3)}°)`:key;
        const feature=metadata.features.find(f=>f.properties.id===city.country_id);
        if(feature)entries.push({label,feature,city});
      });populate();
    }catch(error){el('notice').textContent='City lookup unavailable. Select a country to compare years.';console.error(error);}
  }catch(error){el('notice').textContent='Comparison could not load. Please reload the page.';console.error(error);}
})();
