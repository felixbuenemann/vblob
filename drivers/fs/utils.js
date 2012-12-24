var crypto = require('crypto');

exports.seq_id_cmp = function (seq1, seq2)
{
  var epoch1, epoch2, cnt1, cnt2;
  epoch1 = parseInt(seq1.substr(0, seq1.indexOf('-')),10);
  epoch2 = parseInt(seq2.substr(0, seq2.indexOf('-')),10);
  if (epoch1 < epoch2) return -1;
  if (epoch1 > epoch2) return 1;
  cnt1 = parseInt(seq1.substring(seq1.indexOf('-')+1), 10);
  cnt2 = parseInt(seq2.substring(seq2.indexOf('-')+1), 10);
  if (cnt1 < cnt2) return -1;
  if (cnt1 > cnt2) return 1;
  return 0;
}

function get_key_md5_hash(filename)
{
  var md5_name = crypto.createHash('md5');
  md5_name.update(filename);
  return md5_name.digest('hex');
}

//<md5 hash of the key>-<prefix of the key>-<suffix of the key>
exports.get_key_fingerprint = function (filename)
{
  var digest = get_key_md5_hash(filename);
  var prefix, suffix;
  var file2 = filename.replace(/(\+|=|\^|#|\{|\}|\(|\)|\[|\]|%|\||,|:|!|;|\/|\$|&|@|\*|`|'|"|<|>|\?|\\)/g, "_"); //replacing all special chars with "_"
  if (file2.length < 8) {
    while (file2.length < 8) file2 += '0';
    prefix = file2.substr(0,8);
    suffix = file2.substr(file2.length - 8);
  } else {
    prefix = file2.substr(0,8);
    suffix = file2.substr(file2.length-8);
  }
  return digest+'-'+prefix+'-'+suffix;
}

function hex_val(ch)
{
  if (48 <= ch && ch <= 57) { return ch - 48; }
  return ch - 97 + 10;
}

exports.hex2base64 = function (hex_str)
{
  hex_str = hex_str.toLowerCase();
  var result = "";
  var va = new Array(8);
  var ca = new Array(8);
  for (var idx = 0; idx < hex_str.length; )
  {
    for (var idx2 = 0; idx2 < 6; idx2++)
    {
      if (idx+idx2 < hex_str.length) {
        va[idx2] = hex_str.charCodeAt(idx2+idx);
        va[idx2] = hex_val(va[idx2]);
      } else { va[idx2] = 0; }
    }
    ca[0] = base64_char_table.charAt((va[0] << 2) + (va[1] >> 2));
    ca[1] = base64_char_table.charAt(((va[1]&0x03)<<4)+va[2]);
    ca[2] = base64_char_table.charAt((va[3] << 2) + (va[4] >> 2));
    ca[3] = base64_char_table.charAt(((va[4]&0x03)<<4)+va[5]);
    if (idx + 5 < hex_str.length) {
      //normal case
      result += (ca[0]+ca[1]+ca[2]+ca[3]);
    } else if (idx + 3 < hex_str.length) {
      //padding 1
      result += (ca[0]+ca[1]+ca[2]+"=");
    } else {
      //padding 2
      result += (ca[0]+ca[1]+"==");
    }
    idx += 6;
  }
  return result;
}
