const AUTH_TOKEN = "<YOUR SECRET>";

function doPost(e) {
  var to_url = e.parameter["t"];
  var auth_token = e.parameter["a"];

  if (!to_url) {
    return ContentService
      .createTextOutput("Missing 't' query parameter")
      .setMimeType(ContentService.MimeType.TEXT);
  }

  if (auth_token !== AUTH_TOKEN) {
    return ContentService
      .createTextOutput("Unauthorized")
      .setMimeType(ContentService.MimeType.TEXT);
  }

  var payload = e.postData.contents;
  var contentType = e.postData.type || "application/octet-stream";

  var options = {
    method: "post",
    payload: payload,
    contentType: contentType,
    muteHttpExceptions: true
  };

  try {
    var response = UrlFetchApp.fetch(to_url, options);

    var responseBody = response.getContentText();
    return ContentService
      .createTextOutput(responseBody)
      .setMimeType(ContentService.MimeType.TEXT);
  } catch (error) {
    return ContentService
      .createTextOutput("Proxy error: " + error.toString())
      .setMimeType(ContentService.MimeType.TEXT);
  }
}
