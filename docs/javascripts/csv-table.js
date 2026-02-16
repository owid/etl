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
  // Add toggle button for detail columns
  if (detailCols.length) {
    var btn = document.createElement("button");
    btn.className = "csv-table-toggle";
    btn.textContent = "Show details";
    btn.addEventListener("click", function () {
      var t = container.querySelector("table");
      var on = t.classList.toggle("show-details");
      btn.textContent = on ? "Hide details" : "Show details";
    });
    container.appendChild(btn);
  }

  // Scrollable wrapper
  var wrap = document.createElement("div");
  wrap.className = "csv-table-scroll";

  var table = document.createElement("table");

  // Header
  var thead = document.createElement("thead");
  var hRow = document.createElement("tr");
  headers.forEach(function (h) {
    var th = document.createElement("th");
    th.textContent = h;
    if (detailCols.indexOf(h) >= 0) th.classList.add("detail-col");
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
      if (detailCols.indexOf(h) >= 0) td.classList.add("detail-col");
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  wrap.appendChild(table);
  container.appendChild(wrap);

  // Enable sorting via tablesort (already loaded by the site)
  if (typeof Tablesort !== "undefined") new Tablesort(table);
}
