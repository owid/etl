document$.subscribe(function () {
  document.querySelectorAll(".csv-table[data-src]").forEach(function (el) {
    // Skip if already rendered
    if (el.querySelector("table")) return;

    var src = el.getAttribute("data-src");
    var detailCols = (el.getAttribute("data-detail-columns") || "")
      .split(",")
      .map(function (s) { return s.trim(); })
      .filter(Boolean);

    fetch(new URL(src, window.location.href).href)
      .then(function (r) { return r.text(); })
      .then(function (text) {
        var parsed = Papa.parse(text.trim(), { header: true, skipEmptyLines: true });
        buildCsvTable(el, parsed.meta.fields, parsed.data, detailCols);
      });
  });
});

function buildCsvTable(container, headers, rows, detailCols) {
  var wrap = document.createElement("div");
  wrap.className = "csv-table-scroll";

  var table = document.createElement("table");

  // Header
  var thead = document.createElement("thead");
  var hRow = document.createElement("tr");
  headers.forEach(function (h) {
    var th = document.createElement("th");
    th.textContent = h;
    var idx = detailCols.indexOf(h);
    if (idx === 0) th.classList.add("detail-col-peek");
    else if (idx > 0) th.classList.add("detail-col");
    hRow.appendChild(th);
  });
  thead.appendChild(hRow);
  table.appendChild(thead);

  // Body
  var tbody = document.createElement("tbody");
  rows.forEach(function (row) {
    var tr = document.createElement("tr");
    headers.forEach(function (h) {
      var td = document.createElement("td");
      td.textContent = row[h] || "";
      var idx = detailCols.indexOf(h);
      if (idx === 0) td.classList.add("detail-col-peek");
      else if (idx > 0) td.classList.add("detail-col");
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  wrap.appendChild(table);
  container.appendChild(wrap);

  // Right fade — column toggle
  if (detailCols.length) {
    var fade = document.createElement("div");
    fade.className = "csv-table-fade";
    fade.title = "Show more columns";

    var icon = document.createElement("div");
    icon.className = "csv-fade-icon";
    icon.textContent = "+";
    fade.appendChild(icon);

    fade.addEventListener("click", function () {
      var on = table.classList.toggle("show-details");
      container.classList.toggle("details-visible");
      icon.textContent = on ? "\u00d7" : "+";
      fade.title = on ? "Hide detail columns" : "Show more columns";
    });

    container.appendChild(fade);
  }

  // Bottom fade — decorative scroll hint, hides when scrolled to bottom
  var bottomFade = document.createElement("div");
  bottomFade.className = "csv-table-bottom-fade";
  container.appendChild(bottomFade);

  wrap.addEventListener("scroll", function () {
    var atBottom = wrap.scrollHeight - wrap.scrollTop - wrap.clientHeight < 10;
    bottomFade.style.opacity = atBottom ? "0" : "1";
  });

  // Enable sorting via tablesort (already loaded by the site)
  if (typeof Tablesort !== "undefined") new Tablesort(table);
}
