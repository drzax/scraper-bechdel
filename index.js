var cheerio, sqlite, Promise, request,
	$, q, db;

cheerio = require('cheerio');
sqlite = require('sqlite3').verbose();
Promise = require('promise');
request = require('request');
queue = require('queue-async');

const bechdel = 'http://bechdeltest.com/?list=all';

var fields = [
	{name: 'id', type: 'TEXT PRIMARY KEY'},
	{name: 'title', type: 'TEXT'},
	{name: 'score', type: 'INT'},
	{name: 'dubious', type: 'BOOLEAN'},
	{name: 'year', type: 'INT'},
	{name: 'ratingUS', type: 'TEXT'},
	{name: 'ratingAU', type: 'TEXT'},
	{name: 'runtime', type: 'TEXT'},
	{name: 'genre', type: 'TEXT'},
	{name: 'poster', type: 'TEXT'},
	{name: 'metascore', type: 'TEXT'},
	{name: 'stars', type: 'FLOAT'},
	{name: 'votes', type: 'INT'}
];

// Know we have a database
db = new Promise(function(resolve, reject) {
	var db = new sqlite.Database("data.sqlite");
	db.run("CREATE TABLE IF NOT EXISTS data (" + fields.map(function(d){ return d.name + ' ' + d.type; }).join(', ') + ")", function(err){
		if (err) {
			reject(err);
		} else {
			resolve(db);
		}
	});
});

// Kick off by requesting all movies from bechdeltest.com
console.log('Fetching movie list...');
request.get(bechdel, processList);

// Process the bechdeltest.com listing
function processList(err, response, body) {

	var q, $list;

	// Give up if there's an error here.
	if (err) {
		return handleError(err);
	}

	// Only process 5 movies at a time to avoid flooding all the networks
	q = queue(5);

	// Parse the page
	$ = cheerio.load(body);
	$list = $('.list .movie');
	console.log('List of ' + $list.length + ' movies fetched. Queueing...');
	$list.each(function() {

		var $movie = $(this),
			data = {};

		data.$id = $movie.children().eq(0).attr('href').match(/title\/(.+)\//)[1];
		data.$score = $movie.children().eq(0).find('img').attr('alt').match(/\d+/)[0];
		data.$dubious = !!$movie.children().eq(0).find('img').attr('title').match(/although dubious/);
		data.$title = $movie.children().eq(1).text();

		q.defer(completeMovie, data);
	});

}

// Fetch additional data for the movie requested.
function completeMovie(data, cb) {
	var q = queue();

	console.log('Fetching additional data for ' + data.$title);

	// Get the OMDb data
	q.defer(fetchOmdb, data);

	// Get Australian rating data
	q.defer(fetchAusRating, data);

	// Wait for the deferreds and save to the db
	q.await(function(err, data, b){
		if (err) {
			handleError(err);
		} else {
			saveMovie(data);
		}
		cb();
	});
}

function fetchAusRating(data, cb) {
	request('http://www.classification.gov.au/Pages/Results.aspx?q=' + data.$title + '&t=f', function(err, res, body){
		var $;

		if (err) {
			return cb(err);
		}

		$ = cheerio.load(body);
		data.$ratingAU = $('#ClassificationList td.item-rating img').first().attr('alt');

		console.log('Fetched Australian rating for ' + data.$title);

		cb(null, data);
	});
}

function fetchOmdb(data, cb) {
	request('http://www.omdbapi.com/?i='+data.$id, function(err, res, body){
		var omdbData;

		if (err) {
			return cb(err);
		}

		try {
			omdbData = JSON.parse(body);
		} catch (e) {
			return cb(e);
		}

		data.$title = omdbData.Title;
		data.$year = omdbData.Year;
		data.$ratingUS = omdbData.Rated;
		data.$runtime = omdbData.Runtime;
		data.$genre = omdbData.Genre;
		data.$poster = omdbData.Poster;
		data.$metascore = omdbData.Metascore;
		data.$stars = omdbData.imdbRating;
		data.$votes = omdbData.imdbVotes;

		console.log('Fetched OMDb data for ' + data.$title);

		cb(null, data);
	});
}

function saveMovie(data) {
	db.then(function(db){
		var query = 'INSERT OR REPLACE INTO data (' + fields.map(function(d){return d.name; }).join(', ') + ') VALUES (' + fields.map(function(d){return '$'+d.name; }).join(', ') + ')';
		db.run(query, data);
	});
}

function handleError(err) {
	console.log(err);
}