import 'lucos_navbar';
import Yasgui from "@zazuko/yasgui";
import "@zazuko/yasgui/build/yasgui.min.css";

// yasgui includes any search params in requests to sparql endpoint, so ensure there are none
if (window.location.search) {
	window.history.pushState(true, null, window.location.pathname);
}
new Yasgui(document.getElementById("yasgui"), {
	requestConfig: { endpoint: window.location.origin+"/sparql" },
	copyEndpointOnNewTab: false,
});