/* @bruin
name: staging.shop_type_crosswalk
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Maps each city's native activity code to one of seven canonical shop types, and records how
  cleanly that mapping can be made. This is the single most consequential file in the pipeline:
  every competitor count, every site score and every caveat on the map derives from it.

  It is written as a literal VALUES list rather than generated from patterns so that each
  classification decision is visible in a diff and can be argued with. Every code here was
  read off the live source, not from documentation.

  Grain is (city, native_code, shop_type). The mapping is deliberately many-to-many: where a
  source cannot separate two shop types, the shared code is mapped to both. Paris CH403
  "Bar ou Cafe sans tabac" is a bar competitor and a cafe competitor simultaneously, and
  pretending otherwise would understate one of them. Consumers must therefore count DISTINCT
  establishments, never sum rows across shop types.

  Notable decisions, each of which changes a competitor count materially:

  1. Madrid BAR RESTAURANTE (4,548 open, larger than any pure bar class) is assigned to
     restaurant, not bar. The basis is the publisher's own taxonomy rather than a coin flip:
     epigrafe 561004 sits in CNAE division 5610 "Restaurantes y puestos de comidas" alongside
     RESTAURANTE and CAFETERIA, while the pure bar classes sit in 5630 "Establecimientos de
     bebidas". Assigning it to bar instead would raise Madrid's bar count from 6,444 to
     10,992, a 71% swing.
  2. Madrid bar_pub is BAR CON COCINA, BAR SIN COCINA, TABERNA and the two BAR ESPECIAL
     classes, which totals the 6,444 open bars quoted in the appraisal.
  3. Paris nightclub includes SA404 "Discothequeue et club prive". The appraisal in PLAN.md
     concluded Paris had no nightclub class because it searched only the CH restauration
     block; SA404 sits in "Services culturels et de loisirs" instead. Paris bookstores are
     likewise CE101 "Librairie", also outside the CH block. Both are corrections.
  4. Chicago nightclub is proxied by the Late Hour licence, a permit to serve liquor until
     4am, rather than by Public Place of Amusement as PLAN.md suggested. PPA also covers
     bowling alleys, art studios and private event venues, so it is far too broad; a 4am
     liquor permit is close to a definition of a nightclub.
  5. Chicago cafes are partly separable after all, via the activity token "Preparation and
     Sale of Coffee and/or Drinks". PLAN.md concluded they were not, having looked only at
     license_description. The token "Preparation of Food, Coffee or Drinks" is deliberately
     excluded because it cannot distinguish a coffee shop from a kitchen.
  6. Ice cream, chocolate and tea rooms are assigned to cafe wherever they are separable.
     This is done to improve comparability, not by accident: Mexico City's SCIAN 722515
     merges cafes with juice bars and ice cream parlours and cannot be split, so grouping
     them the same way in Madrid and Paris makes the four cafe universes closer to
     like-for-like than excluding them would.
  7. London bakery is recorded as not separable, correcting the score of 74 in PLAN.md's
     matrix. FHRS publishes 14 business types and none is a bakery; bakeries sit inside
     "Retailers - other" with 16,676 other shops. That is the same situation as London
     bookstores, which PLAN.md correctly marked as no coverage.

depends:
  - raw.madrid_activities
  - raw.paris_bdcom_nomenclature
  - raw.cdmx_denue_units
  - raw.london_fhrs_establishments
  - raw.chicago_business_licenses

materialization:
  type: table
  strategy: create+replace

columns:
  - name: city
    type: VARCHAR
    description: City slug, one of madrid, paris, mexico_city, london, chicago.
    primary_key: true
    nullable: false
  - name: native_code
    type: VARCHAR
    description: Source activity code. Madrid epigrafe_id, Paris codact, Mexico City SCIAN, London BusinessTypeId, Chicago activity token or licence description.
    primary_key: true
    nullable: false
  - name: shop_type
    type: VARCHAR
    description: Canonical shop type, one of bar_pub, nightclub, cafe, restaurant, fast_food, bakery, bookstore.
    primary_key: true
    nullable: false
  - name: native_label
    type: VARCHAR
    description: Source label for the code, in the source language, as published.
  - name: separability
    type: VARCHAR
    description: How cleanly the code isolates the shop type. clean, shared_class, over_broad, partial or not_separable.
  - name: caveat
    type: VARCHAR
    description: Plain-language limitation for this city and shop type, rendered on the map footnote.
  - name: is_usable
    type: BOOLEAN
    description: False where separability is not_separable, meaning the combination must not be scored or mapped.

@bruin */

WITH mapping AS (
    SELECT * FROM UNNEST([

    -- ================================================================= MADRID
    -- Municipal register, epigrafe_id. CNAE 5610 is food service, 5630 is drink service.
    STRUCT('madrid' AS city, '561005' AS native_code, 'bar_pub' AS shop_type, 'BAR CON COCINA' AS native_label, 'clean' AS separability, 'Bar with a kitchen. A dedicated licence class, distinct from BAR RESTAURANTE.' AS caveat),
    ('madrid', '563005', 'bar_pub', 'BAR SIN COCINA', 'clean', 'Bar without a kitchen. A dedicated licence class.'),
    ('madrid', '563004', 'bar_pub', 'TABERNA', 'clean', 'Traditional tavern, its own licence class.'),
    ('madrid', '563002', 'bar_pub', 'BAR ESPECIAL SIN ACTUACIONES', 'clean', 'Late-licence bar without live performance.'),
    ('madrid', '563003', 'bar_pub', 'BAR ESPECIAL CON ACTUACIONES', 'clean', 'Late-licence bar with live performance.'),

    ('madrid', '932006', 'nightclub', 'DISCOTECAS Y SALAS DE BAILE', 'clean', 'Nightclubs and dance halls, a dedicated licence class.'),
    ('madrid', '932005', 'nightclub', 'SALAS DE FIESTA SIN RESTAURACION', 'clean', 'Function venue without food service.'),
    ('madrid', '932004', 'nightclub', 'SALAS DE FIESTA CON RESTAURACION', 'clean', 'Function venue with food service.'),
    ('madrid', '563007', 'nightclub', 'CAFE ESPECTACULO', 'shared_class', 'Cafe-theatre. Licensed as a drink establishment with performance, so it sits between a bar and a nightclub.'),

    ('madrid', '561006', 'cafe', 'CAFETERIA', 'clean', 'Cafe, a dedicated licence class distinct from both bars and restaurants.'),
    ('madrid', '561007', 'cafe', 'CHOCOLATERIA/SALON DE TE Y HELADERIA', 'shared_class', 'Chocolate shop, tea room and ice cream parlour share one class. Grouped with cafes to match Mexico City SCIAN 722515, which cannot separate them.'),
    ('madrid', '472902', 'cafe', 'COMERCIO AL POR MENOR DE HELADOS CON OBRADOR CON BARRA DE DEGUSTACION', 'clean', 'Ice cream maker with a tasting counter, so consumption happens on site.'),

    ('madrid', '561001', 'restaurant', 'RESTAURANTE', 'clean', 'Restaurant, a dedicated licence class.'),
    ('madrid', '561004', 'restaurant', 'BAR RESTAURANTE', 'shared_class', 'Genuine bar-restaurant hybrid and, at 4,548 open premises, larger than any pure bar class. Assigned to restaurant because epigrafe 561004 sits in CNAE 5610 food service, not 5630 drink service. Assigning it to bars instead would raise the Madrid bar count from 6,444 to 10,992.'),
    ('madrid', '561003', 'restaurant', 'AUTOSERVICIO DE RESTAURACION', 'clean', 'Self-service restaurant.'),

    ('madrid', '561002', 'fast_food', 'RESTAURANTES DE COMIDA RAPIDA', 'clean', 'Fast-food restaurant, a dedicated licence class.'),

    ('madrid', '472401', 'bakery', 'COMERCIO AL POR MENOR DE PAN Y PRODUCTOS DE PANADERIA Y BOLLERIA CON OBRADOR', 'clean', 'Bakery that bakes on site (with obrador).'),
    ('madrid', '472402', 'bakery', 'COMERCIO AL POR MENOR DE PAN Y PRODUCTOS DE PANADERIA Y BOLLERIA SIN OBRADOR', 'clean', 'Bread shop with no on-site oven.'),
    ('madrid', '472403', 'bakery', 'COMERCIO AL POR MENOR DE PASTELERIA, CONFITERIA, REPOSTERIA CON OBRADOR-BARRA DEGUSTACION', 'clean', 'Patisserie baking on site with a tasting counter.'),
    ('madrid', '472404', 'bakery', 'COMERCIO AL POR MENOR DE PASTELERIA, CONFITERIA, REPOSTERIA CON OBRADOR-SIN BARRA DEGUSTACION', 'clean', 'Patisserie baking on site without a tasting counter.'),
    ('madrid', '472405', 'bakery', 'COMERCIO AL POR MENOR DE PASTELERIA, CONFITERIA, REPOSTERIA SIN OBRADOR', 'clean', 'Patisserie with no on-site oven.'),
    ('madrid', '472903', 'bakery', 'COMERCIO AL POR MENOR DE HELADOS CON OBRADOR SIN BARRA DE DEGUSTACION', 'clean', 'Ice cream maker without a tasting counter, so it is a production and takeaway shop.'),

    ('madrid', '476101', 'bookstore', 'COMERCIO AL POR MENOR DE LIBROS', 'clean', 'Book retail, a dedicated licence class. Excludes wholesale (464902) and publishing (581001).'),

    -- ================================================================== PARIS
    -- APUR BDCOM, codact in the 220-post nomenclature.
    ('paris', 'CH403', 'bar_pub', 'Bar ou Cafe sans tabac', 'shared_class', 'The nomenclature has one class for bars and cafes together, so the two universes cannot be separated. This code is counted as both a bar and a cafe. It is the single reason Paris scores lower for bars than its base score would suggest.'),
    ('paris', 'CH402', 'bar_pub', 'Cafe - Tabac', 'shared_class', 'Cafe with a tobacco licence. Also counted as a cafe, for the same reason as CH403.'),

    ('paris', 'SA404', 'nightclub', 'Discotheque et club prive', 'clean', 'A dedicated nightclub class. It sits in the leisure-services block rather than the restauration block, which is why an appraisal that searched only the CH codes concluded Paris had no nightclub class.'),
    ('paris', 'CH501', 'nightclub', 'Cabaret - Diner-Spectacle', 'shared_class', 'Cabaret and dinner-show venue, adjacent to but not the same as a nightclub.'),

    ('paris', 'CH401', 'cafe', 'Salon de the', 'clean', 'Tea room, a dedicated class.'),
    ('paris', 'CH403', 'cafe', 'Bar ou Cafe sans tabac', 'shared_class', 'Shared with bar_pub: the nomenclature cannot separate a bar from a cafe.'),
    ('paris', 'CH402', 'cafe', 'Cafe - Tabac', 'shared_class', 'Shared with bar_pub.'),
    ('paris', 'CA113', 'cafe', 'Glacier : vente a emporter et consommation sur place', 'clean', 'Ice cream parlour with on-site consumption. Grouped with cafes to match Mexico City SCIAN 722515.'),

    ('paris', 'CH101', 'restaurant', 'Restaurant traditionnel francais', 'clean', 'One of nine restaurant classes split by cuisine.'),
    ('paris', 'CH102', 'restaurant', 'Restaurant antillais', 'clean', 'One of nine restaurant classes split by cuisine.'),
    ('paris', 'CH103', 'restaurant', 'Restaurant asiatique', 'clean', 'One of nine restaurant classes split by cuisine.'),
    ('paris', 'CH104', 'restaurant', 'Restaurant maghrebin', 'clean', 'One of nine restaurant classes split by cuisine.'),
    ('paris', 'CH105', 'restaurant', 'Restaurant africain', 'clean', 'One of nine restaurant classes split by cuisine.'),
    ('paris', 'CH106', 'restaurant', 'Restaurant europeen', 'clean', 'One of nine restaurant classes split by cuisine.'),
    ('paris', 'CH107', 'restaurant', 'Restaurant central et sud americain', 'clean', 'One of nine restaurant classes split by cuisine.'),
    ('paris', 'CH108', 'restaurant', 'Restaurant indien, pakistanais et Moyen Orient', 'clean', 'One of nine restaurant classes split by cuisine.'),
    ('paris', 'CH109', 'restaurant', 'Autre restaurant du monde', 'clean', 'One of nine restaurant classes split by cuisine.'),
    ('paris', 'CH201', 'restaurant', 'Brasserie - Restauration continue sans tabac', 'clean', 'Brasserie serving continuously, distinct from both bars and traditional restaurants.'),
    ('paris', 'CH202', 'restaurant', 'Brasserie - Restauration continue avec tabac', 'clean', 'Brasserie with a tobacco licence.'),

    ('paris', 'CH302', 'fast_food', 'Restauration rapide debout', 'clean', 'Fast food, standing. Paris is the only city here that separates standing from seated fast food.'),
    ('paris', 'CH303', 'fast_food', 'Restauration rapide assise', 'clean', 'Fast food, seated.'),
    ('paris', 'CH301', 'fast_food', 'Cafeteria', 'clean', 'Cafeteria. The publisher aggregates this to "Restauration rapide" at 47 posts, so it is treated as fast food rather than as a cafe.'),

    ('paris', 'CA103', 'bakery', 'Boulangerie - Boulangerie Patisserie', 'clean', 'Bakery, the largest food-retail class in Paris.'),
    ('paris', 'CA104', 'bakery', 'Patisserie', 'clean', 'Patisserie, separate from bakery.'),
    ('paris', 'CA105', 'bakery', 'Chocolaterie - Confiserie', 'clean', 'Chocolate shop and confectioner.'),

    ('paris', 'CE101', 'bookstore', 'Librairie', 'clean', 'Bookshop, a dedicated class. It sits in the "Librairie - Journaux" retail block rather than the restauration block. Excludes CE304 antiquarian books, which the publisher classes with art galleries and collectors.'),

    -- ============================================================ MEXICO CITY
    -- INEGI DENUE, 6-digit SCIAN.
    ('mexico_city', '722412', 'bar_pub', 'Bares, cantinas y similares', 'clean', 'A dedicated SCIAN class. But 1,043 registered bars for 9.2M residents against Madrid 6,444 for 3.3M is a 17-fold per-capita gap. That is under-registration and informality, not a real difference in drinking culture of that magnitude. Treat bar density here as a lower bound and do not compare it to the other four cities without saying so.'),

    ('mexico_city', '722411', 'nightclub', 'Centros nocturnos, discotecas y similares', 'clean', 'A dedicated SCIAN class, but only 71 establishments citywide. Subject to the same under-registration as bars, and more severely.'),

    ('mexico_city', '722515', 'cafe', 'Cafeterias, fuentes de sodas, neverias, refresquerias y similares', 'shared_class', 'Cafes, soda fountains, ice cream parlours and juice bars share one SCIAN class and cannot be separated, so the cafe universe is inflated relative to a strict coffee-shop definition. This is why ice cream and tea rooms are grouped with cafes in the other cities too.'),

    ('mexico_city', '722511', 'restaurant', 'Restaurantes con servicio de preparacion de alimentos a la carta o de comida corrida', 'clean', 'A la carte and set-menu restaurants.'),
    ('mexico_city', '722512', 'restaurant', 'Restaurantes con servicio de preparacion de pescados y mariscos', 'clean', 'Fish and seafood restaurants.'),
    ('mexico_city', '722513', 'restaurant', 'Restaurantes con servicio de preparacion de antojitos', 'clean', 'Antojitos restaurants, a sit-down format.'),
    ('mexico_city', '722514', 'restaurant', 'Restaurantes con servicio de preparacion de tacos y tortas', 'clean', 'Taquerias, the single largest food-service class in the city.'),

    ('mexico_city', '722516', 'fast_food', 'Restaurantes de autoservicio', 'clean', 'Self-service restaurants.'),
    ('mexico_city', '722517', 'fast_food', 'Restaurantes con servicio de preparacion de pizzas, hamburguesas, hot dogs y pollos rostizados para llevar', 'clean', 'Takeaway pizza, burgers, hot dogs and rotisserie chicken.'),
    ('mexico_city', '722518', 'fast_food', 'Restaurantes que preparan otro tipo de alimentos para llevar', 'clean', 'Other takeaway food preparation.'),
    ('mexico_city', '722519', 'fast_food', 'Servicios de preparacion de otros alimentos para consumo inmediato', 'clean', 'Other immediate-consumption food preparation, close to street food in practice.'),

    ('mexico_city', '311812', 'bakery', 'Panificacion tradicional', 'clean', 'Traditional bakery. DENUE classes bakeries under manufacturing rather than retail because a Mexican panaderia bakes on the premises, so this code will be missed by anyone searching only the retail SCIAN range.'),
    ('mexico_city', '311811', 'bakery', 'Panificacion industrial', 'clean', 'Industrial bakery, only 14 establishments.'),

    ('mexico_city', '465312', 'bookstore', 'Comercio al por menor de libros', 'clean', 'Book retail, a dedicated SCIAN class. Excludes wholesale (433420) and publishing (513131, 513132).'),

    -- ================================================================= LONDON
    -- FSA FHRS, BusinessTypeId.
    ('london', '7843', 'bar_pub', 'Pub/bar/nightclub', 'shared_class', 'One FHRS class covers pubs, bars and nightclubs together, so bars cannot be separated from nightclubs. Counted as both.'),
    ('london', '7843', 'nightclub', 'Pub/bar/nightclub', 'shared_class', 'Shared with bar_pub. FHRS has no separate nightclub type, so London nightclub counts are not a real nightclub universe.'),

    ('london', '1', 'cafe', 'Restaurant/Cafe/Canteen', 'shared_class', 'One FHRS class covers restaurants, cafes and canteens together. Cafes cannot be separated without name heuristics, so the cafe count here is really the whole eat-in universe and is inflated several-fold.'),
    ('london', '1', 'restaurant', 'Restaurant/Cafe/Canteen', 'shared_class', 'Shared with cafe, and also includes workplace and school canteens, which are not competitors for a commercial site.'),

    ('london', '7844', 'fast_food', 'Takeaway/sandwich shop', 'clean', 'Takeaways and sandwich shops, a dedicated FHRS class and the cleanest London category.'),

    ('london', '4613', 'bakery', 'Retailers - other', 'not_separable', 'FHRS publishes 14 business types and none is a bakery. Bakeries sit inside "Retailers - other" alongside 16,676 unrelated shops, so there is no bakery universe to count. This corrects the score of 74 in the original appraisal matrix.'),
    ('london', '4613', 'bookstore', 'Retailers - other', 'not_separable', 'FHRS is a food-hygiene register. Bookshops appear only if they also sell food, and are not distinguishable from other retailers. No coverage.'),

    -- ================================================================ CHICAGO
    -- BACP licences. native_code is an activity token from the pipe-delimited
    -- business_activity field, except where noted as a licence description.
    ('chicago', 'Tavern - Consumption of Liquor on Premises', 'bar_pub', 'Tavern - Consumption of Liquor on Premises', 'clean', 'The tavern activity token gives a clean bar universe. The much larger "Consumption of Liquor on Premises" token (2,896) is deliberately excluded because it marks restaurants holding a liquor licence, not bars.'),

    ('chicago', 'Sale of Liquor Until 4am, Monday - Saturday and 5am on Sunday', 'nightclub', 'Late Hour licence, liquor until 4am', 'partial', 'Chicago has no nightclub licence, so the Late Hour permit to serve liquor until 4am is used as the proxy. This is narrower and more accurate than Public Place of Amusement, which the original plan suggested but which also covers bowling alleys, art studios and private event venues.'),

    ('chicago', 'Preparation and Sale of Coffee and/or Drinks', 'cafe', 'Preparation and Sale of Coffee and/or Drinks', 'partial', 'Cafes are partly separable via this activity token, contrary to the original appraisal, which looked only at license_description. It is a lower bound: an independent coffee shop that registered only as a Retail Food Establishment without this token is invisible. The token "Preparation of Food, Coffee or Drinks" (491) is excluded because it cannot distinguish a coffee shop from a kitchen.'),

    ('chicago', 'Preparation of Food and Dining on Premises With Seating', 'restaurant', 'Preparation of Food and Dining on Premises With Seating', 'clean', 'Seated dining, the core restaurant token.'),
    ('chicago', 'Sale of Food Prepared Onsite With Dining Area', 'restaurant', 'Sale of Food Prepared Onsite With Dining Area', 'clean', 'Food prepared on site with a dining area.'),
    ('chicago', 'Expedited Restaurant with On-Premises Consumption', 'restaurant', 'Expedited Restaurant with On-Premises Consumption', 'clean', 'Expedited restaurant licence with on-site consumption.'),

    ('chicago', 'Sale of Food Prepared Onsite Without Dining Area', 'fast_food', 'Sale of Food Prepared Onsite Without Dining Area', 'clean', 'Food prepared on site with no dining area, the takeaway equivalent.'),
    ('chicago', 'Expedited Restaurant without On-Premises Consumption', 'fast_food', 'Expedited Restaurant without On-Premises Consumption', 'clean', 'Expedited restaurant licence without on-site consumption.'),

    ('chicago', 'Operation of a Deli, Butcher or Bakery', 'bakery', 'Operation of a Deli, Butcher or Bakery', 'over_broad', 'Delis, butchers and bakeries share one activity token, so a bakery cannot be isolated. At 168 licences the token is also far too small to be the real bakery universe. Reported but flagged as over-broad.'),

    ('chicago', 'Sale of Books', 'bookstore', 'Sale of Books', 'partial', 'Only 21 licences carry this token and 17 more carry "Sale of Used Books", against a real Chicago bookshop count several times higher. Most book retailers hold a Limited Business License with no activity token, so this is not a usable universe.'),
    ('chicago', 'Sale of Used Books', 'bookstore', 'Sale of Used Books', 'partial', 'Second-hand book retail. Same limitation as "Sale of Books".')

    ])
)

SELECT
    city,
    native_code,
    shop_type,
    native_label,
    separability,
    caveat,
    separability != 'not_separable' AS is_usable
FROM mapping
ORDER BY city, shop_type, native_code
