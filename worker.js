var browscap = require('browscap'),
	csv = require('csv'),
	path = require('path'),
	nano = require('nano')('http://127.0.0.1:5984/'),
	request = require('request'),
	async = require('async-mini'),
	moment = require('moment'),
	url = require('url');

var search_hash = {},
	search_list = [];

function ip2long(IP) {
	var i;

	IP = IP.match(/^([1-9]\d*|0[0-7]*|0x[\da-f]+)(?:\.([1-9]\d*|0[0-7]*|0x[\da-f]+))?(?:\.([1-9]\d*|0[0-7]*|0x[\da-f]+))?(?:\.([1-9]\d*|0[0-7]*|0x[\da-f]+))?$/i);
	if (!IP) {
		return false;
	}

	IP[0] = 0;
	for (i = 1; i < 5; i += 1) {
		IP[0] += !! ((IP[i] || '').length);
		IP[i] = parseInt(IP[i]) || 0;
	}

	IP.push(256, 256, 256, 256);

	IP[4 + IP[0]] *= Math.pow(256, 4 - IP[0]);

	if (IP[1] >= IP[5] || IP[2] >= IP[6] || IP[3] >= IP[7] || IP[4] >= IP[8]) {
		return false;
	}

	return IP[1] * (IP[0] === 1 || 16777216) + IP[2] * (IP[0] <= 2 || 65536) + IP[3] * (IP[0] <= 3 || 256) + IP[4] * 1;
}

function search(ip) {
	var key = null,
		val = ip2long(ip);

	search_list.some(function (v) {
		if (v > val) {
			key = v;
			return true;
		}

		return false;
	});

	if (!key) {
		return 'unknown';
	}

	return search_hash[key];
}

var provider_tzs = {};

async.parallel({
	browscap: function (callback) {
		browscap.setIni(path.join(__dirname, 'browscap.ini'));
		callback(null);
	},
	csv_geo: function (callback) {
		csv()
			.from.path(path.join(__dirname, 'GeoIPCountryWhois.csv'), { delimiter: ',', escape: '"' })
			.to.array(function (data) {
				// caching country data
				data.forEach(function (v) {
					var iplong = parseInt(v[2]);
					search_hash[iplong] = v[5];
					search_list.push(iplong);
				});

				callback(null);
			});
	},
	provider_tzs: function (callback) {
		request({
			url: 'https://www.nextadnet.com/update/providers/',
			strictSSL: false,
			json: true
		}, function (error, res, json) {
			callback(error);

			provider_tzs = json;


		});
	}
}, function (error) {
	if (error) {
		console.log('Error: %j', error);
		return;
	}

	var clicks = nano.db.use('clicks'),
		for_update = [];

	clicks.view('processing', 'not_processed', { limit: 10000 }, function (error, body) {
		if (error) {
			console.log('Couch error: %j', error);
			return;
		}

		body.rows.forEach(function (row) {
			var obj = row.value,
				dt = moment.unix(obj.timestamp);

			if (!(obj.providerid in provider_tzs)) {
				console.log('Wrong providerid %d. All: %j', obj.providerid, provider_tzs);
				return;
			}

			obj.utc_hour = dt.hour();
			obj.utc_date = dt.format('YYYY-MM-DD');

			dt.zone(provider_tzs[obj.providerid]);

			obj.provider_hour = dt.hour();
			obj.provider_date = dt.format('YYYY-MM-DD');

			obj.country = search(obj.userip);
			obj.hostname = url.parse(obj.referer).hostname;

			var browser = browscap.getBrowser(obj.useragent);
			obj.browser = browser['Browser'];
			obj.browser_ver = browser['Version'];
			obj.platform = browser['Platform'];

			obj.language = obj.acceptlanguage ? obj.acceptlanguage.split(',')[0].split('-')[0] : '';

			obj.is_processed = true;

			for_update.push(obj);
		});

		clicks.bulk({ docs: for_update }, function (error) {
			if (error) {
				console.log('Error: %j', error);
			}
		});
	});
});



