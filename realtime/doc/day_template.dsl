PUT _template/gmall2020_dau_info_template
{
  "index_patterns": [
    "gmall2020_dau_info*"
  ],
  "aliases": {
    "{index}-query": {},
    "gmall2020_dau_info-query": {}
  },
  "mappings": {
    "properties": {
      "mid": {
        "type": "keyword"
      },
      "uid": {
        "type": "keyword"
      },
      "ar": {
        "type": "keyword"
      },
      "ch": {
        "type": "keyword"
      },
      "vc": {
        "type": "keyword"
      },
      "dt": {
        "type": "keyword"
      },
      "hr": {
        "type": "keyword"
      },
      "mi": {
        "type": "keyword"
      },
      "ts": {
        "type": "date"
      }
    }
  }
}