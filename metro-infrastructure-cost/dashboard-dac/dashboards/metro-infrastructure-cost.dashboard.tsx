const regionFilter = {
  name: "region",
  type: "select",
  multiple: true,
  default: ["North America", "Europe"],
  options: { values: ["North America", "Europe", "Other"] },
}

const sourceFootnote = (limitations) => `**Sources:** **[Transit Costs Project](https://transitcosts.com/data/)** (project costs, route length, alignment, stations, dates, and source references); **[World Bank WDI](https://data.worldbank.org/)** (GDP per capita PPP, population, and urbanization context).

**Tools:** **Bruin cli** (ingestion, transformations, and report tables), **BigQuery** (warehouse), **Bruin dac** (visualization).

**Limitations:** ${limitations}`

export default (
  <Dashboard
    name="Metro Infrastructure Cost Benchmark"
    description="A comparable, source-aware benchmark of urban-rail construction cost per corridor kilometer and its observable project drivers."
    connection="bruin-playground-arsalan"
    theme="ibm-cb-dark"
  >
    <Filter {...regionFilter} />

    <Row>
      <Text
        name="Introduction"
        col={12}
        content={`# What makes an urban rail kilometer expensive?

**This benchmark compares metro and urban-rail project costs in 2025 USD millions per corridor kilometer, then tests how much of the spread is associated with tunneling, line length, station density, construction duration, and city context. The default view is North America and Europe; use the region filter to inspect other cities included in the source snapshot.**

Cost per kilometer is a screening metric, not a complete engineering estimate. Each chart keeps the project identity, source quality, and limitations visible so large values can be investigated rather than treated as universal city rankings.`}
      />
    </Row>

    <Row>
      <Metric
        name="Included projects"
        col={3}
        sql={include("queries/kpis.sql")}
        value={{ field: "included_projects", type: "number", format: ",.0f" }}
      />
      <Metric
        name="Cities represented"
        col={3}
        sql={include("queries/kpis.sql")}
        value={{ field: "included_cities", type: "number", format: ",.0f" }}
      />
      <Metric
        name="Included route-km"
        col={3}
        sql={include("queries/kpis.sql")}
        value={{ field: "route_km", type: "number", format: ",.0f" }}
      />
      <Metric
        name="Weighted average cost/km"
        col={3}
        sql={include("queries/kpis.sql")}
        value={{ field: "weighted_avg_cost_per_km", type: "number", format: "$,.0f" }}
      />
    </Row>

    <Row>
      <Divider name="City and scale" col={12} />
    </Row>

    <Row>
      <Text
        name="City cost header"
        col={12}
        content={`# Length-weighted construction cost per kilometer by city

**City is the primary comparison unit here. Each bar aggregates included project phases within a city or metropolitan area and weights cost/km by route length; the companion median helps distinguish a repeated city pattern from a single expensive phase.**

**Encoding key:** horizontal position = city; bar length = weighted average cost per corridor kilometer (2025 USD millions); tooltip fields = country, project count, route-km, and median cost/km.`}
      />
    </Row>
    <Row height="600px">
      <Chart
        name="City weighted cost per kilometer"
        hideName={true}
        type="chart"
        chart="bar"
        horizontal={true}
        col={12}
        sql={include("queries/city_cost.sql")}
        x={{ field: "city_label", type: "category" }}
        y={{ field: ["weighted_avg_cost_per_km_2025_usd_m"], type: "number", format: "$,.0f", beginAtZero: true }}
      />
    </Row>
    <Row>
      <Text name="City cost footnote" col={12} content={sourceFootnote("The chart shows the 20 highest city-level averages in the selected regions to keep labels readable; country is retained in the tooltip and audit table. City coverage is selective, not a census of all projects. Weighted averages can be driven by a few large projects, while medians are unweighted. Costs vary in scope and may include different treatment of systems, facilities, land, taxes, or rolling stock.")} />
    </Row>

    <Row>
      <Text
        name="Tunnel cost header"
        col={12}
        content={`# Cost per kilometer versus tunnel share

**Across the 199 default-region projects with usable values, tunnel share and cost/km have only a weak pooled linear association (Pearson r = 0.15); the relationship is stronger in North America (r = 0.57) than Europe (r = 0.28). The cloud is descriptive and does not isolate tunneling from station complexity, soil conditions, procurement, or governance.**

**Encoding key:** x-position = tunnel share (%); y-position = cost per corridor kilometer (2025 USD millions); each point = one included project.`}
      />
    </Row>
    <Row height="430px">
      <Chart
        name="Tunnel share and cost per kilometer"
        hideName={true}
        type="chart"
        chart="scatter"
        col={12}
        sql={include("queries/tunnel_cost.sql")}
        x={{ field: "tunnel_pct", type: "number", title: "Tunnel share (%)", format: ".0f" }}
        y={{ field: ["cost_per_km_2025_usd_m"], type: "number", title: "Cost per corridor km (2025 USD millions)", format: "$,.0f" }}
      />
    </Row>
    <Row>
      <Text
        name="Tunnel cost footnote"
        col={12}
        content={sourceFootnote("Scatter tooltips expose numeric coordinates; the outlier table below supplies project names and source references. Tunnel percentage is missing for a small subset and those projects are omitted from this chart.")}
      />
    </Row>

    <Row>
      <Divider name="Project design and delivery drivers" col={12} />
    </Row>

    <Row>
      <Text
        name="Length cost header"
        col={12}
        content={`# Cost per kilometer versus project length

**There is essentially no pooled linear relationship between corridor length and cost/km in the default view (Pearson r = -0.06 across 199 projects). North America is more negative (r = -0.34), while Europe is near zero (r = -0.03), so any apparent scale effect is region-sensitive.**

**Encoding key:** x-position = corridor length (km); y-position = cost per corridor kilometer (2025 USD millions); each point = one included project.`}
      />
    </Row>
    <Row height="430px">
      <Chart
        name="Project length and cost per kilometer"
        hideName={true}
        type="chart"
        chart="scatter"
        col={12}
        sql={include("queries/length_cost.sql")}
        x={{ field: "length_km", type: "number", title: "Corridor length (km)", format: ",.0f" }}
        y={{ field: ["cost_per_km_2025_usd_m"], type: "number", title: "Cost per corridor km (2025 USD millions)", format: "$,.0f" }}
      />
    </Row>
    <Row>
      <Text
        name="Length cost footnote"
        col={12}
        content={sourceFootnote("Length is corridor length, not single-track length. A scatter plot is used because the question concerns the shape of a continuous relationship; named projects are available in the outlier table.")}
      />
    </Row>

    <Row>
      <Text
        name="Station density header"
        col={12}
        content={`# Cost per kilometer versus station density

**Station density shows no pooled linear signal against cost/km in the default view (Pearson r = -0.02 across 199 projects). It remains useful as a diagnostic because station size, depth, and complexity are not standardized in the source.**

**Encoding key:** x-position = stations per corridor kilometer; y-position = cost per corridor kilometer (2025 USD millions); each point = one included project.`}
      />
    </Row>
    <Row height="430px">
      <Chart
        name="Station density and cost per kilometer"
        hideName={true}
        type="chart"
        chart="scatter"
        col={12}
        sql={include("queries/station_density_cost.sql")}
        x={{ field: "stations_per_km", type: "number", title: "Stations per corridor km", format: ".2f" }}
        y={{ field: ["cost_per_km_2025_usd_m"], type: "number", title: "Cost per corridor km (2025 USD millions)", format: "$,.0f" }}
      />
    </Row>
    <Row>
      <Text
        name="Station density footnote"
        col={12}
        content={sourceFootnote("Projects without a station count are omitted. Stations are source-attributed project counts and may not be comparable in size, depth, or scope across countries.")}
      />
    </Row>

    <Row>
      <Text
        name="Duration cost header"
        col={12}
        content={`# Construction duration versus cost per kilometer

**Longer construction periods have a weak positive pooled association with cost/km (Pearson r = 0.22 across 199 projects), with a stronger signal in North America (r = 0.44) than Europe (r = 0.29). Duration is calculated from source start and end years; it is not a schedule-overrun measure.**

**Encoding key:** x-position = construction duration (years); y-position = cost per corridor kilometer (2025 USD millions); each point = one included project.`}
      />
    </Row>
    <Row height="430px">
      <Chart
        name="Construction duration and cost per kilometer"
        hideName={true}
        type="chart"
        chart="scatter"
        col={12}
        sql={include("queries/duration_cost.sql")}
        x={{ field: "construction_duration_years", type: "number", title: "Construction duration (years)", format: ".1f" }}
        y={{ field: ["cost_per_km_2025_usd_m"], type: "number", title: "Cost per corridor km (2025 USD millions)", format: "$,.0f" }}
      />
    </Row>
    <Row>
      <Text
        name="Duration cost footnote"
        col={12}
        content={sourceFootnote("Only projects with valid start and end years are plotted. Multi-phase projects can make elapsed duration look longer than the delivery time of an individual phase.")}
      />
    </Row>

    <Row>
      <Text
        name="Decade header"
        col={12}
        content={`# Length-weighted cost per kilometer by construction decade

**The default-region weighted average rises from $254M/km in the 2000s and $256M/km in the 2010s to $389M/km in the 2020s, while the 2020s sample is 55 projects and 546 route-km. The visible project count and route-km are essential context because project mix changes by decade.**

**Encoding key:** x-position = construction-start decade; bar height = length-weighted cost per corridor kilometer (2025 USD millions).`}
      />
    </Row>
    <Row height="420px">
      <Chart
        name="Cost per kilometer by construction decade"
        hideName={true}
        type="chart"
        chart="bar"
        col={12}
        sql={include("queries/decade_cost.sql")}
        x={{ field: "construction_decade", type: "category" }}
        y={{ field: ["weighted_avg_cost_per_km_2025_usd_m"], type: "number", title: "Cost per corridor km (2025 USD millions)", format: "$,.0f", beginAtZero: true }}
      />
    </Row>
    <Row>
      <Text
        name="Decade footnote"
        col={12}
        content={sourceFootnote("Costs are normalized to 2025 dollars by the source. City and project mix changes over time, so this is not a pure construction-inflation index. Decades with missing start years are omitted.")}
      />
    </Row>

    <Row>
      <Divider name="Outliers and evidence" col={12} />
    </Row>
    <Row>
      <Text
        name="Outlier table header"
        col={12}
        content={`# Highest-cost included projects in the selected regions

**The table is the audit layer for the charts: inspect the city, project scope, tunnel share, station count, construction dates, source-quality labels, and reference URL before interpreting a city average as a general rule.**`}
      />
    </Row>
    <Row>
      <Table
        name="Included project outliers"
        col={12}
        sql={include("queries/outliers.sql")}
        columns={[
          { name: "project_label", label: "Project", frozen: true },
          { name: "city", label: "City" },
          { name: "country_name", label: "Country" },
          { name: "analysis_region", label: "Region" },
          { name: "length_km", label: "Length (km)", number: ",.1f" },
          { name: "tunnel_pct", label: "Tunnel (%)", number: ".1f" },
          { name: "stations", label: "Stations", number: ",.0f" },
          { name: "construction_duration_years", label: "Duration (years)", number: ".1f" },
          { name: "cost_per_km_2025_usd_m", label: "Cost/km (2025 $M)", number: "$,.0f" },
          { name: "cost_per_station_2025_usd_m", label: "Cost/station (2025 $M)", number: "$,.0f" },
          { name: "cost_source_type", label: "Cost source" },
          { name: "source_quality_score", label: "Source score", number: ",.0f" },
          { name: "source_length_type", label: "Length source" },
          { name: "context_year_gap", label: "Context gap (years)", number: ",.0f" },
          { name: "reference_url", label: "Reference" },
        ]}
      />
    </Row>

    <Row>
      <Text
        name="Methodology"
        col={12}
        content={`# Methodology

**Population:** The raw Transit Costs Project snapshot is retained globally. Dashboard metrics include rows classified as \`included_metro_urban_rail\` with positive corridor length and positive 2025 cost/km. Regional/mainline, high-speed, BRT, tram/streetcar, trolley, and LRT keyword matches remain available in staging but are excluded from comparisons.

**Normalization:** Cost/km uses the source's PPP and inflation-adjusted 2025 USD millions. Corridor length is the source's revenue alignment length, not track length. City averages are weighted by route-km; project medians are unweighted.

**Context join:** Each project is joined to the nearest World Bank country-year observation around its construction midpoint. The selected year and absolute year gap are displayed so historical context is not presented as contemporaneous city-level data.

**Interpretation:** These are descriptive benchmarks, not causal estimates or engineering cost forecasts. The data cannot isolate labor, soil, procurement, station size, land acquisition, financing, taxes, rolling stock, maintenance facilities, or governance effects consistently across countries.`}
      />
    </Row>
  </Dashboard>
)
