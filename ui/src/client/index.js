import 'lucos_navbar';
import Yasgui from "@triply/yasgui";
import "@triply/yasgui/build/yasgui.min.css";

const yasgui = new Yasgui(document.getElementById("yasgui"), {
  requestConfig: { endpoint: "/sparql" },
  copyEndpointOnNewTab: false,
});