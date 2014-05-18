function handlebars(template, data) {
  if(!handlebars.compiled.hasOwnProperty(template)) {
      handlebars.compiled[template] = Handlebars.compile($(template).html());
    }
    return $.trim(handlebars.compiled[template](data));
}

handlebars.compiled = {};
 