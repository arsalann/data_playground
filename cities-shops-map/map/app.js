/* Shop density across five cities, measured on official business registers.
 *
 * Reads the GeoJSON and JSON files written by export_geojson.py and renders three layers per
 * city and shop-type combination: observed shops per 1,000 residents over the 250 m grid,
 * existing premises as points, and (Paris only) surveyed vacant units.
 *
 * Nothing here is modelled. An earlier version scored each cell on where to open a new shop;
 * that was the only predictive element in the project and it has been removed.
 *
 * Accessibility choices, per VISUALIZATIONS.md section 11:
 *  - Density uses a single-hue sequential blue ramp, safe under every form of colour vision
 *    deficiency, and every bin carries its real numeric range in the legend, read from
 *    report.density_bins rather than labelled by rank.
 *  - The two non-ramp classes are separated by hue as well as position: white for "none
 *    within 400 m" and grey for "too few residents to rate".
 *  - Premise points encode classification certainty as filled versus hollow, so the
 *    data-quality warning does not depend on colour.
 *  - Vacant units are diamonds with churn risk encoded as halo weight as well as colour, and
 *    they avoid vermillion entirely so they cannot be confused with the premise points.
 *  - The map is readable without hovering: title, insight, legend and caveats are all static.
 */

'use strict';

const DATA = 'data/';

const BASEMAP_STYLE = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';

/* Density classes. -1 and 0 are qualitatively different from the ramp, so they get their own
   colours rather than the pale end of it: a cell with no shops and a cell whose ratio cannot
   be published are not "low density". */
const CLASS_COLORS = {
  '-1': '#c9ced3',
  0: '#ffffff',
  1: '#d6e7f4',
  2: '#93c4e0',
  3: '#3f8fc0',
  4: '#08508a',
};
const CLASS_ORDER = [-1, 0, 1, 2, 3, 4];

/* Shading for the data-quality matrix, which scores 50-100 rather than a density. */
const MATRIX_SHADES = ['#eff6fb', '#c6def0', '#8fc3e3', '#3e90c4', '#00538a'];

/* Wong (2011) colourblind-safe categorical palette. Keep this in sync with style.css :root. */
const WONG = {
  orange: '#e69f00',
  skyblue: '#56b4e9',
  green: '#009e73',
  blue: '#0072b2',
  vermillion: '#d55e00',
  purple: '#cc79a7',
};

const CITY_ORDER = ['madrid', 'paris', 'mexico_city', 'london', 'chicago'];
const TYPE_ORDER = [
  'bar_pub', 'nightclub', 'cafe', 'restaurant', 'fast_food', 'bakery', 'bookstore',
];

/* Separability grades that mean "this point might not actually be this shop type". */
const UNCERTAIN = new Set(['shared_class', 'over_broad', 'partial']);

const state = {
  matrix: [],
  matrixIndex: new Map(),
  cities: {},
  manifest: {},
  osmGap: [],
  bins: {},
  city: 'madrid',
  shopType: 'cafe',
  map: null,
  loaded: false,
  vacantLoaded: false,
  token: 0,
};

/* ------------------------------------------------------------------ helpers */

const key = (city, shopType) => `${city}|${shopType}`;

const num = (value) => (value === null || value === undefined ? '-' : Number(value).toLocaleString('en-GB'));

/* Ratios span three orders of magnitude across combinations (0.05 to 306), so fix the
   significant figures rather than the decimal places. */
const fmtRatio = (value) => {
  if (value === null || value === undefined) return '-';
  const n = Number(value);
  return n < 10 ? n.toFixed(2) : n.toFixed(1);
};

function esc(text) {
  return String(text ?? '').replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

async function getJSON(path) {
  const response = await fetch(DATA + path);
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return response.json();
}

function setLoading(on) {
  document.getElementById('loading').hidden = !on;
}

/* Toggling a layer that failed to be added throws and aborts whatever else the caller was
   doing, so one bad paint expression takes the whole page down. Check first. */
function setVisibility(layerId, visible) {
  if (state.map && state.map.getLayer(layerId)) {
    state.map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
  } else {
    console.warn(`Layer ${layerId} is missing, cannot set visibility`);
  }
}

function meta() {
  return state.matrixIndex.get(key(state.city, state.shopType));
}

/* report.coverage_matrix ships the caveats as a JSON array, one entry per source code, so
   there is no concatenated string to split back apart. */
function splitCaveats(row) {
  if (!row) return [];
  try {
    const parsed = JSON.parse(row.caveats_json || '[]');
    if (Array.isArray(parsed) && parsed.length) return parsed;
  } catch (error) {
    console.warn('Could not parse caveats_json', error);
  }
  return row.caveats ? [row.caveats] : [];
}

/* -------------------------------------------------------------- bootstrap */

async function init() {
  [state.matrix, state.cities, state.manifest, state.osmGap, state.bins] = await Promise.all([
    getJSON('coverage_matrix.json'),
    getJSON('cities.json'),
    getJSON('manifest.json'),
    getJSON('osm_gap.json'),
    getJSON('density_bins.json'),
  ]);

  state.matrix.forEach((row) => {
    state.matrixIndex.set(key(row.city, row.shop_type), row);
  });

  buildCitySelect();
  buildTypeSelect();
  buildMatrixView();
  buildSubscoreTable();
  buildMethodView();
  wireTabs();
  wireControls();

  state.map = new maplibregl.Map({
    container: 'map',
    style: BASEMAP_STYLE,
    center: [-3.70, 40.42],
    zoom: 11,
    attributionControl: { compact: false },
  });
  state.map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-left');
  state.map.addControl(new maplibregl.ScaleControl({ maxWidth: 120, unit: 'metric' }), 'bottom-left');

  state.map.on('load', () => {
    addLayers();
    state.loaded = true;
    refresh({ fit: true });
  });

  buildLegend();
  renderTextPanels();
}

function buildCitySelect() {
  const select = document.getElementById('city-select');
  CITY_ORDER.filter((c) => state.manifest[c]).forEach((city) => {
    const row = state.matrix.find((r) => r.city === city);
    const option = document.createElement('option');
    option.value = city;
    option.textContent = row ? row.city_label : city;
    select.appendChild(option);
  });
  select.value = state.city;
}

function buildTypeSelect() {
  const select = document.getElementById('type-select');
  const available = state.manifest[state.city] || [];
  select.innerHTML = '';
  TYPE_ORDER.filter((t) => available.includes(t)).forEach((shopType) => {
    const row = state.matrixIndex.get(key(state.city, shopType));
    const option = document.createElement('option');
    option.value = shopType;
    option.textContent = row ? row.shop_type_label : shopType;
    select.appendChild(option);
  });
  if (!available.includes(state.shopType)) {
    state.shopType = TYPE_ORDER.find((t) => available.includes(t));
  }
  select.value = state.shopType;
}

function wireTabs() {
  document.querySelectorAll('.tab').forEach((tab) => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach((t) => {
        t.classList.toggle('is-active', t === tab);
        t.setAttribute('aria-selected', String(t === tab));
      });
      document.querySelectorAll('.view').forEach((view) => {
        view.classList.toggle('is-active', view.id === `view-${tab.dataset.view}`);
      });
      if (tab.dataset.view === 'map' && state.map) {
        // The container was display:none, so MapLibre needs to re-measure it.
        setTimeout(() => state.map.resize(), 0);
      }
    });
  });
}

function wireControls() {
  document.getElementById('city-select').addEventListener('change', (event) => {
    state.city = event.target.value;
    buildTypeSelect();
    refresh({ fit: true });
  });
  document.getElementById('type-select').addEventListener('change', (event) => {
    state.shopType = event.target.value;
    refresh({ fit: false });
  });
  document.getElementById('toggle-scores').addEventListener('change', (event) => {
    ['density-fill', 'density-line'].forEach((id) => setVisibility(id, event.target.checked));
  });
  document.getElementById('toggle-points').addEventListener('change', (event) => {
    ['points-clean', 'points-uncertain'].forEach((id) => setVisibility(id, event.target.checked));
  });
  document.getElementById('toggle-vacant').addEventListener('change', async (event) => {
    if (event.target.checked && !state.vacantLoaded) await loadVacant();
    setVisibility('vacant', event.target.checked);
  });
}

/* ---------------------------------------------------------------- map layers */

const EMPTY = { type: 'FeatureCollection', features: [] };

function addLayers() {
  const map = state.map;

  map.addSource('density', { type: 'geojson', data: EMPTY });
  map.addSource('points', { type: 'geojson', data: EMPTY });
  map.addSource('vacant', { type: 'geojson', data: EMPTY });

  map.addLayer({
    id: 'density-fill',
    type: 'fill',
    source: 'density',
    paint: {
      'fill-color': [
        'match', ['get', 'c'],
        -1, CLASS_COLORS['-1'],
        0, CLASS_COLORS[0],
        1, CLASS_COLORS[1],
        2, CLASS_COLORS[2],
        3, CLASS_COLORS[3],
        4, CLASS_COLORS[4],
        '#cccccc',
      ],
      'fill-opacity': 0.74,
    },
  });

  /* A hairline outline keeps the 250 m grid legible as a grid rather than reading as a
     continuous surface, which would overstate the spatial precision of the measure. It also
     makes the white "none within 400 m" cells visible instead of invisible. */
  map.addLayer({
    id: 'density-line',
    type: 'line',
    source: 'density',
    paint: {
      'line-color': '#8d959c',
      'line-width': ['interpolate', ['linear'], ['zoom'], 11, 0.1, 15, 0.6],
      'line-opacity': 0.35,
    },
  });

  /* Premise points recede at overview zoom and come forward as you zoom in.
     Without this, London's 24,585 eat-in premises render as a solid orange mass that hides
     the choropleth completely - the site-score surface is the analysis, and the premises are
     the evidence behind it, so the surface has to win at city scale. */
  const POINT_RADIUS = ['interpolate', ['linear'], ['zoom'], 11, 1.3, 13, 2.4, 15, 4.6, 17, 7.5];
  const POINT_OPACITY = ['interpolate', ['linear'], ['zoom'], 11, 0.38, 13, 0.62, 15, 0.9];

  map.addLayer({
    id: 'points-clean',
    type: 'circle',
    source: 'points',
    filter: ['!', ['in', ['get', 'sep'], ['literal', [...UNCERTAIN]]]],
    paint: {
      'circle-radius': POINT_RADIUS,
      'circle-color': WONG.vermillion,
      'circle-opacity': POINT_OPACITY,
      // No halo at overview zoom: a white ring doubles the apparent footprint of each dot.
      'circle-stroke-width': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 0.7],
      'circle-stroke-color': '#ffffff',
    },
  });

  /* Hollow rings, so an uncertain classification is distinguishable by shape alone. */
  map.addLayer({
    id: 'points-uncertain',
    type: 'circle',
    source: 'points',
    filter: ['in', ['get', 'sep'], ['literal', [...UNCERTAIN]]],
    paint: {
      'circle-radius': POINT_RADIUS,
      'circle-color': 'rgba(255,255,255,0.30)',
      'circle-stroke-width': ['interpolate', ['linear'], ['zoom'], 11, 0.7, 13, 1.0, 15, 1.7],
      'circle-stroke-color': WONG.vermillion,
      'circle-stroke-opacity': POINT_OPACITY,
    },
  });

  /* Vacant units are the actionable output, so they sit on top and stay visible at every zoom.
     Their palette avoids vermillion entirely: the premise points are already vermillion, and
     an orange diamond next to an orange ring is not a distinguishable encoding at city scale.
     Churn risk runs purple to orange to black, which also reads as increasing severity.

     Drawn as registered images rather than a text glyph. A symbol layer with
     'text-field': '◆' renders nothing, because U+25C6 is not in the basemap's glyph set and
     MapLibre silently draws no label when a glyph is missing. */
  registerDiamondIcons(map);

  map.addLayer({
    id: 'vacant',
    type: 'symbol',
    source: 'vacant',
    layout: {
      'icon-image': [
        'match', ['get', 'risk'],
        'high', 'diamond-high',
        'elevated', 'diamond-mid',
        'moderate', 'diamond-mid',
        'diamond-low',
      ],
      'icon-size': ['interpolate', ['linear'], ['zoom'], 11, 0.42, 14, 0.68, 17, 1.0],
      'icon-allow-overlap': true,
      'icon-ignore-placement': true,
      visibility: 'none',
    },
  });

  wirePopups();
}

/* Build the three churn-risk diamonds on a canvas and register them with the map, so the shape
   channel does not depend on the basemap's font coverage. Halo weight carries churn risk as a
   second channel alongside fill colour. */
function registerDiamondIcons(map) {
  const SIZE = 30;
  const variants = [
    { id: 'diamond-low', fill: WONG.purple, stroke: '#ffffff', outline: '#6f3f5c', width: 2 },
    { id: 'diamond-mid', fill: WONG.orange, stroke: '#ffffff', outline: '#7a5400', width: 3 },
    { id: 'diamond-high', fill: '#000000', stroke: '#ffffff', outline: '#000000', width: 4 },
  ];

  variants.forEach(({ id, fill, stroke, outline, width }) => {
    const canvas = document.createElement('canvas');
    canvas.width = SIZE;
    canvas.height = SIZE;
    const ctx = canvas.getContext('2d');
    const half = SIZE / 2;
    const reach = half - width;

    ctx.beginPath();
    ctx.moveTo(half, half - reach);
    ctx.lineTo(half + reach, half);
    ctx.lineTo(half, half + reach);
    ctx.lineTo(half - reach, half);
    ctx.closePath();

    // White halo first so the diamond stays legible over a dark choropleth cell.
    ctx.lineWidth = width;
    ctx.strokeStyle = stroke;
    ctx.stroke();
    ctx.fillStyle = fill;
    ctx.fill();
    ctx.lineWidth = 1;
    ctx.strokeStyle = outline;
    ctx.stroke();

    const { data } = ctx.getImageData(0, 0, SIZE, SIZE);
    map.addImage(id, { width: SIZE, height: SIZE, data: new Uint8Array(data) }, { pixelRatio: 2 });
  });
}

function wirePopups() {
  const map = state.map;
  const popup = new maplibregl.Popup({ closeButton: true, closeOnClick: true, maxWidth: '300px' });

  ['points-clean', 'points-uncertain', 'vacant', 'density-fill'].forEach((layer) => {
    map.on('mouseenter', layer, () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', layer, () => { map.getCanvas().style.cursor = ''; });
  });

  map.on('click', (event) => {
    const hits = map.queryRenderedFeatures(event.point, {
      layers: ['vacant', 'points-clean', 'points-uncertain', 'density-fill'],
    });
    if (!hits.length) return;
    const hit = hits[0];
    const html = hit.layer.id === 'vacant'
      ? vacantPopup(hit.properties)
      : hit.layer.id === 'density-fill'
        ? cellPopup(hit.properties)
        : pointPopup(hit.properties);
    popup.setLngLat(event.lngLat).setHTML(html).addTo(map);
  });
}

function row(label, value) {
  return `<div class="popup-row"><span>${label}</span><span>${value}</span></div>`;
}

function cellPopup(p) {
  const info = meta();
  const type = info.shop_type_label.toLowerCase();

  if (p.c === -1) {
    return `
      <div class="popup-title">Too few residents to rate</div>
      ${row('Residents within 400 m', num(p.pop))}
      ${row(`${info.shop_type_label} within 400 m`, num(p.shops))}
      ${row('All shops within 400 m', num(p.allshops))}
      <div class="popup-note">Below 500 residents within 400 m, a per-resident ratio says more about the small denominator than about the neighbourhood, so it is withheld. This is typically a workplace or industrial district.</div>
    `;
  }

  if (p.c === 0) {
    return `
      <div class="popup-title">No ${esc(type)} within 400 m</div>
      ${row('Residents within 400 m', num(p.pop))}
      ${row('All shops within 400 m', num(p.allshops))}
      <div class="popup-note">250 m cell. The register records no ${esc(type)} in the surrounding 400 m. Check the caveats below the map before reading that as genuine absence.</div>
    `;
  }

  return `
    <div class="popup-title">${Number(p.per1k).toFixed(2)} ${esc(type)} per 1,000 residents
      <span style="font-weight:400">(rank ${num(p.rank)} in ${esc(info.city_label)})</span>
    </div>
    ${row(`${info.shop_type_label} within 400 m`, num(p.shops))}
    ${row('Residents within 400 m', num(p.pop))}
    ${row('All shops within 400 m', num(p.allshops))}
    ${row('Density class', `${p.c} of 4`)}
    <div class="popup-note">250 m cell. Both figures are counts within 400 m of the cell centre, so the ratio describes the neighbourhood rather than the cell.</div>
  `;
}


function pointPopup(p) {
  const uncertain = UNCERTAIN.has(p.sep);
  return `
    <div class="popup-title">${esc(p.name || 'Name not published')}</div>
    ${row('Source class', esc(p.label || '-'))}
    ${row('Classification', esc(p.sep))}
    ${uncertain
      ? '<div class="popup-note">The source class is shared with another shop type or is broader than it, so this premise may not be the selected type.</div>'
      : '<div class="popup-note">The source has a dedicated class for this shop type.</div>'}
  `;
}

function vacantPopup(p) {
  const per1k = p[`per1k_${state.shopType}`];
  const shops = p[`shops_${state.shopType}`];
  const info = meta();
  return `
    <div class="popup-title">${esc(p.addr || 'Address not published')}</div>
    ${row('Arrondissement', num(p.arr))}
    ${row('Vacancy', esc(p.why))}
    ${row('Floor area', p.m2 ? `${num(p.m2)} m²` : esc(p.band || 'not recorded'))}
    ${row(`${esc(info.shop_type_label)} within 400 m`, num(shops))}
    ${row('Per 1,000 residents', per1k === undefined ? 'not published' : Number(per1k).toFixed(2))}
    ${row('Activity changes 2000-2020', num(p.churn))}
    ${row('Churn', esc(p.risk))}
    <div class="popup-note">A vacant unit surveyed on foot by APUR in June 2023. Churn counts how often the recorded activity at this address changed across the eight survey waves. The density figures describe the surrounding neighbourhood, not this unit.</div>
  `;
}

/* -------------------------------------------------------------------- data */

async function refresh({ fit }) {
  if (!state.loaded) return;
  const token = ++state.token;
  setLoading(true);

  renderTextPanels();
  buildLegend();

  const isParis = state.city === 'paris';
  document.getElementById('vacant-control').hidden = !isParis;
  document.getElementById('legend-vacant').hidden = !isParis;
  if (!isParis) {
    document.getElementById('toggle-vacant').checked = false;
    setVisibility('vacant', false);
  }

  try {
    const [cells, points] = await Promise.all([
      getJSON(`density/${state.city}__${state.shopType}.geojson`),
      getJSON(`points/${state.city}__${state.shopType}.geojson`),
    ]);
    if (token !== state.token) return;

    state.map.getSource('density').setData(cells);
    state.map.getSource('points').setData(points);

    if (fit) {
      const bounds = state.cities[state.city].bounds;
      state.map.fitBounds([[bounds[0], bounds[1]], [bounds[2], bounds[3]]], {
        padding: 40,
        duration: 700,
      });
    }
  } catch (error) {
    console.error(error);
  } finally {
    if (token === state.token) setLoading(false);
  }
}

async function loadVacant() {
  const data = await getJSON('vacant/paris.geojson');
  state.map.getSource('vacant').setData(data);
  state.vacantLoaded = true;
}

/* ------------------------------------------------------------- text panels */

function renderTextPanels() {
  const info = meta();
  if (!info) return;
  const cells = state.cities[state.city].cells;

  document.getElementById('map-title').textContent =
    `${info.shop_type_label} per 1,000 residents across ${info.city_label}, 250 m cells`;

  const pct = info.pct_geocoded === null ? null : Number(info.pct_geocoded);
  const missing = info.establishments_open - info.establishments_mappable;

  document.getElementById('map-description').innerHTML = `<strong>${
    esc(insightSentence(info, cells, missing, pct))
  }</strong>`;

  document.getElementById('encoding-key').innerHTML = `
    <strong>Cell fill:</strong> ${esc(info.shop_type_label.toLowerCase())} per 1,000 residents
    within 400 m, darker is denser. <strong>White:</strong> none within 400 m.
    <strong>Grey:</strong> under 500 residents within 400 m, so the ratio is withheld.
    <strong>Solid dot:</strong> existing premise with a clean source classification.
    <strong>Hollow ring:</strong> source class is shared or over-broad, so the premise may not be
    this type.
    ${state.city === 'paris' ? '<strong>Diamond:</strong> surveyed vacant unit, colour shows churn. All 8,985 are drawn, so zoom in to read individual addresses.' : ''}
    <strong>Grid:</strong> 250 m cells, identical resolution in all five cities.
    Premise dots are deliberately faint at this zoom so the density surface stays readable; zoom
    in and they come forward.
  `;

  renderQualityBadge(info);
  renderCaveats(info);
  renderFootnote(info, cells, missing, pct);
}

function insightSentence(info, cells, missing, pct) {
  const parts = [];
  const type = info.shop_type_label.toLowerCase();
  const bins = state.bins[key(state.city, state.shopType)] || [];
  const byClass = new Map(bins.map((b) => [b.c, b]));
  const none = byClass.get(0);
  const top = byClass.get(4);

  /* Where the source class is shared, calling the premises "cafes" would overstate what the
     register knows. Name the class instead. */
  const noun = info.granularity <= 5
    ? `premises in the source class covering ${type}`
    : `${type} premises`;
  parts.push(
    `${num(info.establishments_mappable)} ${noun} are mapped across ${num(cells)} cells in ${info.city_label}.`
  );

  if (top && top.min !== null) {
    parts.push(
      `The top class is ${num(top.cells)} cells at ${fmtRatio(top.min)} or more per 1,000 residents, with a median of ${num(top.med_shops)} within 400 m.`
    );
  }
  if (none) {
    parts.push(
      `${num(none.cells)} cells have none within 400 m.`
    );
  }

  if (info.granularity <= 5) {
    parts.push(
      `Read the ratio with care: this register scores ${info.granularity}/10 on category granularity, so the numerator is not a clean ${type} count.`
    );
  } else if (info.coverage <= 4) {
    parts.push(
      `Read the ratio with care: coverage scores ${info.coverage}/10 because this shop type is under-registered here, so the true density is higher than shown.`
    );
  }
  if (missing > 0 && pct !== null) {
    parts.push(
      `A further ${num(missing)} premises exist in the register with no usable coordinates, so ${pct.toFixed(1)}% of the universe is counted.`
    );
  }
  return parts.join(' ');
}

function renderQualityBadge(info) {
  const badge = document.getElementById('quality-badge');
  const score = Number(info.score);
  badge.textContent = `${score.toFixed(0)} / 100`;
  badge.className = 'badge ' + (score >= 88 ? 'badge--strong' : score >= 78 ? 'badge--fair' : 'badge--weak');
  badge.title = `Weighted data-quality score for ${info.city_label} ${info.shop_type_label}`;
}

function renderCaveats(info) {
  const dims = [
    ['Authority', info.authority],
    ['Coverage', info.coverage],
    ['Granularity', info.granularity],
    ['Freshness', info.freshness],
    ['Geocoding', info.geocoding],
    ['Access', info.access],
  ];
  document.getElementById('caveat-scores').innerHTML = dims.map(([label, value]) => `
    <span class="subscore ${value <= 4 ? 'subscore--low' : ''}">${label} <b>${value}</b>/10</span>
  `).join('');

  document.getElementById('caveat-source').innerHTML = `
    <strong>Source:</strong> ${esc(info.source_name)}. <strong>Vintage:</strong> ${esc(info.source_vintage)}.
    <strong>Source classes used:</strong> <code>${esc(info.native_codes || '-')}</code>.
  `;

  const items = splitCaveats(info);
  document.getElementById('caveat-list').innerHTML = items.length
    ? items.map((text) => `<li>${esc(text)}</li>`).join('')
    : '<li>No specific caveats recorded for this combination.</li>';
}

function renderFootnote(info, cells, missing, pct) {
  document.getElementById('map-footnote').innerHTML = `
    <p><strong>Sources:</strong> ${esc(info.source_name)} (establishments, ${esc(info.source_vintage)});
    <strong><a href="https://human-settlement.emergency.copernicus.eu/download.php?ds=pop">JRC Global Human Settlement Layer</a></strong>
    GHS-POP R2023A epoch 2025 at 100 m (resident population);
    basemap <strong><a href="https://carto.com/attributions">CARTO</a></strong> positron.</p>

    <p><strong>Tools:</strong> <strong>Bruin cli</strong> (pipeline), <strong>BigQuery</strong> (warehouse),
    <strong>MapLibre GL JS</strong> (map).</p>

    <p><strong>Limitations:</strong>
    This map measures what is on the ground; it does not recommend anywhere. A dense cell may be
    dense because the location works or because the neighbourhood is saturated, and this data
    cannot tell you which.
    Both numerator and denominator are counts within 400 m of the cell centre, so the ratio
    describes a neighbourhood rather than a 250 m square, and neighbouring cells overlap.
    The ratio is withheld below 500 residents within 400 m: on a denominator that small the
    figure reports the denominator, not the neighbourhood. Those cells are shown in grey and are
    typically workplace or industrial districts.
    The grid is restricted to the ${num(cells)} cells containing at least one geocoded
    establishment of any type, so the map is silent about greenfield locations rather than
    showing them as empty.
    Colour bins are quartiles of this city and shop type, so the same colour means a different
    number in another city; the legend states the real range.
    ${missing > 0 ? `${num(missing)} ${info.shop_type_label.toLowerCase()} premises in the register have no usable coordinates and are absent from the count (${pct === null ? '-' : pct.toFixed(1)}% of the universe is included).` : 'Every premise of this type in the register carries usable coordinates.'}
    Resident population is a modelled disaggregation of census counts onto built-up area, not a
    direct census count, and is more reliable about where people are than exactly how many. It
    counts residents only, so a district serving commuters or tourists reads as over-provided.
    Category caveats specific to this combination are listed above the footnote and are not
    repeated here.</p>
  `;
}

/* ------------------------------------------------------------------ legend */

function buildLegend() {
  const info = meta();
  const bins = state.bins[key(state.city, state.shopType)] || [];
  const byClass = new Map(bins.map((b) => [b.c, b]));

  const label = (cls) => {
    const b = byClass.get(cls);
    if (cls === -1) return ['Too few residents', b ? `${num(b.cells)} cells` : ''];
    if (cls === 0) return ['None within 400 m', b ? `${num(b.cells)} cells` : ''];
    if (!b || b.min === null) return [`Class ${cls}`, ''];
    // Real value ranges, so the reader sees magnitude rather than a rank.
    return [`${fmtRatio(b.min)} - ${fmtRatio(b.max)}`, `${num(b.cells)} cells`];
  };

  document.getElementById('legend-bins').innerHTML = CLASS_ORDER.map((cls) => {
    const [text, note] = label(cls);
    return `
      <li>
        <span class="swatch" style="background:${CLASS_COLORS[String(cls)] || CLASS_COLORS[cls]}"></span>
        <span class="bin-label">${esc(text)}</span>
        <span class="bin-note">${esc(note)}</span>
      </li>
    `;
  }).join('');

  if (info) {
    document.querySelector('.legend h3').textContent =
      `${info.shop_type_label} per 1,000 residents`;
  }
}

/* ------------------------------------------------------------ matrix view */

function buildMatrixView() {
  const cities = CITY_ORDER.filter((c) => state.matrix.some((r) => r.city === c));
  const header = ['Shop type', ...cities.map((c) => state.matrix.find((r) => r.city === c).city_label)];

  const rows = TYPE_ORDER.map((shopType) => {
    const label = state.matrix.find((r) => r.shop_type === shopType)?.shop_type_label || shopType;
    const cells = cities.map((city) => {
      const info = state.matrixIndex.get(key(city, shopType));
      if (!info) return '<td class="cell-score cell-none"><span>-</span></td>';
      const score = Number(info.score);
      const usable = info.is_mappable === true || info.is_mappable === 'true';
      const shade = MATRIX_SHADES[Math.min(4, Math.max(0, Math.floor((score - 50) / 10)))];
      const ink = score >= 80 ? '#ffffff' : '#14171a';
      const title = usable
        ? `${info.establishments_open} open premises, ${info.establishments_mappable} mappable`
        : `Not scoreable: ${info.separability}`;
      return usable
        ? `<td class="cell-score"><span style="background:${shade};color:${ink}" title="${esc(title)}">${score.toFixed(0)}</span></td>`
        : `<td class="cell-score cell-none"><span title="${esc(title)}">${score.toFixed(0)}</span></td>`;
    });
    return `<tr><td>${esc(label)}</td>${cells.join('')}</tr>`;
  });

  document.getElementById('matrix-table').innerHTML = `
    <thead><tr>${header.map((h) => `<th>${esc(h)}</th>`).join('')}</tr></thead>
    <tbody>${rows.join('')}</tbody>
  `;

  const excluded = state.matrix.filter(
    (r) => !(r.is_mappable === true || r.is_mappable === 'true')
  );
  document.getElementById('matrix-footnote').innerHTML = `
    <p><strong>Sources:</strong> Madrid <strong><a href="https://datos.madrid.es/dataset/200085-0-censo-locales">Censo de locales</a></strong> (Ayuntamiento de Madrid, CC BY 4.0);
    Paris <strong><a href="https://opendata.apur.org/datasets/bdcom-2023">APUR BDCOM 2023</a></strong> (ODbL);
    Mexico City <strong><a href="https://www.inegi.org.mx/app/mapa/denue/">INEGI DENUE</a></strong>;
    London <strong><a href="https://api.ratings.food.gov.uk/help">FSA Food Hygiene Rating Scheme</a></strong> (OGL v3.0);
    Chicago <strong><a href="https://data.cityofchicago.org/Community-Economic-Development/Business-Licenses-Current-Active/uupf-x98q">Business Licenses</a></strong> (BACP).</p>

    <p><strong>Tools:</strong> <strong>Bruin cli</strong> (pipeline), <strong>BigQuery</strong> (warehouse), <strong>MapLibre GL JS</strong> (map).</p>

    <p><strong>Limitations:</strong> Scores are a judgement expressed as a rubric, not a measurement.
    They are reproducible because every sub-score is stored in <code>staging.data_quality_scores</code>
    and every count behind them is asserted against the live source, but the weights themselves are a
    choice. ${excluded.length} of ${state.matrix.length} combinations are excluded from the map, either
    because the shop type cannot be separated in that city's taxonomy or because fewer than 50 mappable
    premises remain: ${excluded.map((r) => `${esc(r.city_label)} ${esc(r.shop_type_label.toLowerCase())}`).join(', ')}.
    Establishment counts are of open premises and count each premise once, so a premise that a register
    maps to two shop types is not double counted within a type. Where separability is
    <code>not_separable</code>, the open and mappable counts are the size of the unresolvable source
    bucket rather than of the shop type: London's 16,676 are all "Retailers - other", of which
    bakeries and bookshops are an unknown fraction.</p>
  `;
}

function buildSubscoreTable() {
  const dims = [
    ['authority', 'Authority', 20],
    ['coverage', 'Coverage', 20],
    ['granularity', 'Granularity', 25],
    ['freshness', 'Freshness', 15],
    ['geocoding', 'Geocoding', 10],
    ['access', 'Access', 10],
  ];
  const header = ['City', 'Shop type', ...dims.map(([, label, weight]) => `${label} (${weight})`),
    'Score', 'Open', 'Mappable', 'Separability'];

  const body = CITY_ORDER.flatMap((city) => TYPE_ORDER.map((shopType) => {
    const info = state.matrixIndex.get(key(city, shopType));
    if (!info) return '';
    const cells = dims.map(([field]) => {
      const value = info[field];
      return `<td class="${value <= 4 ? 'sub-low' : ''}">${value}</td>`;
    });
    return `<tr>
      <td>${esc(info.city_label)}</td>
      <td style="text-align:left">${esc(info.shop_type_label)}</td>
      ${cells.join('')}
      <td><b>${Number(info.score).toFixed(0)}</b></td>
      <td>${num(info.establishments_open)}</td>
      <td>${num(info.establishments_mappable)}</td>
      <td style="text-align:left">${esc(info.separability)}</td>
    </tr>`;
  })).filter(Boolean);

  document.getElementById('subscore-table').innerHTML = `
    <thead><tr>${header.map((h) => `<th>${esc(h)}</th>`).join('')}</tr></thead>
    <tbody>${body.join('')}</tbody>
  `;
}

/* ------------------------------------------------------------- method view */

function buildMethodView() {
  const gap = state.osmGap;
  const bars = gap.find((r) => r.shop_type === 'bar_pub');
  const bakery = gap.find((r) => r.shop_type === 'bakery');
  const fast = gap.find((r) => r.shop_type === 'fast_food');

  document.getElementById('osm-insight').innerHTML = `<strong>
    OpenStreetMap holds ${bars ? Number(bars.coverage_ratio).toFixed(2) : '-'} objects for every
    registered Madrid bar, and ${bars ? Number(bars.register_missing_pct).toFixed(1) : '-'}% of
    registered bars have no OpenStreetMap object within 75 m. For bakeries the gap is wider:
    ${bakery ? Number(bakery.register_missing_pct).toFixed(1) : '-'}% are absent. The failure is not
    only an undercount - OpenStreetMap holds
    ${fast ? Number(fast.coverage_ratio).toFixed(2) : '-'}x as many fast-food objects as the register
    has licences, and only ${fast ? Number(fast.osm_matched_pct).toFixed(1) : '-'}% of them sit near a
    registered fast-food premise, because the OpenStreetMap tag is broader than the licence class.
    A shop density built on OpenStreetMap alone cannot distinguish a thinly-served street from an
    unmapped one.</strong>`;

  const header = ['Shop type', 'Registered', 'In OSM', 'Coverage ratio',
    'OSM objects matching a register premise', 'Registered premises absent from OSM'];
  const rows = gap.map((r) => `<tr>
    <td style="text-align:left">${esc(r.shop_type.replace(/_/g, ' '))}</td>
    <td>${num(r.register_count)}</td>
    <td>${num(r.osm_count)}</td>
    <td>${Number(r.coverage_ratio).toFixed(2)}</td>
    <td>${num(r.osm_matched)} (${Number(r.osm_matched_pct).toFixed(1)}%)</td>
    <td><b>${num(r.register_missing_from_osm)} (${Number(r.register_missing_pct).toFixed(1)}%)</b></td>
  </tr>`);

  document.getElementById('osm-table').innerHTML = `
    <thead><tr>${header.map((h) => `<th>${esc(h)}</th>`).join('')}</tr></thead>
    <tbody>${rows.join('')}</tbody>
  `;

  document.getElementById('method-text').innerHTML = `
    <h3>What the map measures</h3>
    <p>One ratio, from two observed counts:</p>
<pre>shops_per_1k_residents = 1000 * shops_within_400m / residents_within_400m</pre>
    <p>An earlier version of this project scored each cell on where to open a new shop, using the
    reference project's method
    (<a href="https://github.com/AzeemPatil/kadikoy-cafe-site-selection">kadikoy-cafe-site-selection</a>):
    a distance-decay competition surface blended 55/45 with a demand proxy. That was the only
    part of this work that predicted anything rather than measuring it, and it has been removed
    along with the two inputs that existed solely to feed it: rail and metro proximity, and the
    complementary-premise term.</p>
    <p>Three choices decide what the remaining ratio says:</p>
    <ul>
      <li><strong>The ratio is withheld below 500 residents within 400 m.</strong> Without a
      floor, a City of London cell with two residents and two cafes reports 937 cafes per 1,000
      residents. Those cells are drawn in grey with their own legend entry, because a workplace
      district with almost no residents is a real category rather than missing data. The floor
      removes 1-4.5% of cells per city.</li>
      <li><strong>Zero is its own class.</strong> A cell with none of a shop type within 400 m is
      qualitatively different from one with few, and binning them together would split identical
      values across colours.</li>
      <li><strong>Bins are value-based, not rank-based.</strong> Two cells with the same ratio
      always get the same colour, which a rank-based split like NTILE cannot guarantee.</li>
    </ul>
    <p>Bins are quartiles within a single city and shop type. The five registers differ in
    completeness, so a shared scale across cities would compare data quality as much as retail
    provision. The legend always states the real value range.</p>

    <h3>The grid</h3>
    <p>One 250 m square grid, built identically for all five cities from an equirectangular metric
    approximation with a per-city latitude reference:</p>
<pre>cell_x = FLOOR(lon * 111320 * COS(lat_ref) / 250)
cell_y = FLOOR(lat * 110540 / 250)</pre>
    <p>Latitude references are Madrid 40.4780, Paris 48.8585, Mexico City 19.3205, London 51.4894,
    Chicago 41.8338, each the centre of the city's bounding box. The grid is restricted to cells
    containing at least one geocoded establishment of any shop type, which is the direct analogue of
    the reference project using street-network nodes rather than a blanket raster: a cell with no
    commercial premises at all is not somewhere this data can describe.</p>

    <h3>Deliberate departures from the original plan</h3>
    <ul>
      <li><strong>One population source, not five.</strong> The plan called for five national
      small-area sources and flagged, as a known limitation, that their vintages differ by up to six
      years. GHS-POP 2025 at 100 m removes that limitation rather than documenting it: same
      estimator, same epoch, same resolution everywhere. The cost is that GHS-POP models where
      people are rather than counting them.</li>
      <li><strong>No transit input at all.</strong> The plan used rail and metro proximity as a
      demand term. With the modelled score gone there is nothing for it to feed, so the asset was
      removed rather than left in the pipeline computing a column nobody reads. It had been built
      from OpenStreetMap for all five cities, and the completeness check it logged came out at
      91-113% of each city's published rapid-transit network.</li>
      <li><strong>Chicago via SoQL, not ingestr.</strong> The repo's Socrata connection returns HTTP
      403 "Invalid app_token specified", so the asset pages the public SoQL endpoint directly. No
      credential is read or required.</li>
    </ul>

    <h3>Corrections found while building</h3>
    <ul>
      <li>Paris has a dedicated nightclub class, <code>SA404 Discotheque et club prive</code>, and a
      dedicated bookshop class, <code>CE101 Librairie</code>. The original appraisal concluded
      otherwise because it searched only the CH restauration block; both codes sit outside it.</li>
      <li>Chicago cafes are partly separable, via the activity token <code>Preparation and Sale of
      Coffee and/or Drinks</code>. The appraisal looked only at <code>license_description</code>.</li>
      <li>London has no bakery class at all. The appraisal scored London bakeries 74; FHRS publishes
      14 business types and bakeries sit inside "Retailers - other" with 16,676 other shops, so the
      real score is 54 and the combination is excluded from the map.</li>
      <li>Madrid uses <code>(0, 0)</code> as its missing-coordinate sentinel on 50,762 of 203,560
      premises. Left unhandled those reproject into the Atlantic. The gap is strongly structured:
      96.6% of street-front premises are geocoded against 30% of premises inside markets and
      galleries.</li>
      <li>Paris <code>surfexacte</code> carries an exact floor area for only 1.9% of premises, not
      the broad coverage the appraisal implied. The banded <code>surf</code> column is the usable
      one.</li>
      <li>Mexico City DENUE includes 20,586 <code>Semifijo</code> units alongside 442,146
      <code>Fijo</code> ones, so it is not the fixed-premises-only register the appraisal described.</li>
    </ul>
  `;

  document.getElementById('method-footnote').innerHTML = `
    <p><strong>Sources:</strong> <strong><a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a></strong>
    via <strong><a href="https://overpass-api.de/">Overpass API</a></strong> (ODbL), tags
    <code>amenity=bar|pub|cafe|restaurant|fast_food|nightclub</code> and <code>shop=bakery|pastry|books</code>;
    Madrid <strong><a href="https://datos.madrid.es/dataset/200085-0-censo-locales">Censo de locales</a></strong>
    (Ayuntamiento de Madrid, CC BY 4.0).</p>

    <p><strong>Tools:</strong> <strong>Bruin cli</strong> (pipeline), <strong>BigQuery</strong> (warehouse,
    <code>ST_DWITHIN</code> for the 75 m match), <strong>MapLibre GL JS</strong> (map).</p>

    <p><strong>Limitations:</strong> The comparison is Madrid only, chosen because its register is the
    strongest of the five and can act as a reference universe rather than another estimate; the gap in
    other cities is unmeasured and may differ. The 75 m match radius is deliberately loose because
    OpenStreetMap nodes are placed by eye while the register geocodes to the building entrance, so a
    tighter radius would measure geocoding precision rather than presence. Neither side is ground
    truth: the register misses businesses trading without a licence, and Madrid's own portal warns the
    data is "an administrative registry without statistical processing" that "may contain
    inconsistencies". A coverage ratio above 1 does not mean OpenStreetMap is more complete, only that
    its tag is broader than the licence class.</p>
  `;
}

init().catch((error) => {
  console.error(error);
  document.body.insertAdjacentHTML('afterbegin',
    `<p style="padding:20px;color:#d55e00"><strong>Could not load map data.</strong>
     Run <code>python3 cities-shops-map/map/export_geojson.py</code> first. (${esc(error.message)})</p>`);
});
