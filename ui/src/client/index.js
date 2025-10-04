import 'lucos_navbar';
import Yasgui from "@zazuko/yasgui";
import "@zazuko/yasgui/build/yasgui.min.css";

// yasgui includes any search params in requests to sparql endpoint, so ensure there are none
if (window.location.search) {
	window.history.pushState(true, null, window.location.pathname);
}
const yasgui_container = document.getElementById("yasgui");
new Yasgui(yasgui_container, {
	requestConfig: {
		endpoint: window.location.origin+"/sparql",
		headers: () => ({
			Authorization: `basic ${yasgui_container.dataset.auth}`,
		}),
	},
	copyEndpointOnNewTab: false,
});