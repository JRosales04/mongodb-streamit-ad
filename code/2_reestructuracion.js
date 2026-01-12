db.facturas.updateMany({}, [
    {
        $set: {
            movies_ref: {
                $map: {
                    input: {
                        $filter: {
                            input: "$Movies",
                            as: "m",
                            cond: {
                                $and: [
                                    { $ne: ["$$m", null] },
                                    {
                                        $ifNull: [
                                            "$$m.Details.imdb_link",
                                            false,
                                        ],
                                    },
                                ],
                            },
                        },
                    },
                    as: "m",
                    in: {
                        mov_id: {
                            $arrayElemAt: [
                                {
                                    $split: [
                                        {
                                            $arrayElemAt: [
                                                {
                                                    $split: [
                                                        "$$m.Details.imdb_link",
                                                        "/",
                                                    ],
                                                },
                                                4,
                                            ],
                                        },
                                        "?",
                                    ],
                                },
                                0,
                            ],
                        },
                        viewing_pct: "$$m.viewing_pct",
                        date_time: "$$m.date_time",
                        License: "$$m.License",
                    },
                },
            },
        },
    },
]);

// Crea la colección Peliculas con el _id = mov_ref extraído de los Movies de facturas

db.facturas.aggregate([
    { $unwind: "$Movies" },
    {
        $match: {
            Movies: { $ne: null },
            "Movies.Details.imdb_link": { $exists: true, $ne: null },
        },
    },
    { $replaceRoot: { newRoot: "$Movies" } },
    {
        $addFields: {
            _id: {
                $arrayElemAt: [
                    {
                        $split: [
                            {
                                $arrayElemAt: [
                                    { $split: ["$Details.imdb_link", "/"] },
                                    4,
                                ],
                            },
                            "?",
                        ],
                    },
                    0,
                ],
            },
        },
    },
    { $unset: ["viewing_pct", "date_time", "License"] },
    { $match: { _id: { $exists: true, $type: "string", $ne: null, $ne: "" } } },
    {
        $merge: {
            into: "Peliculas",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert",
        },
    },
]);

// Elimina el campo 'Movies' en todos los documentos de la colección facturas
db.facturas.updateMany({}, { $unset: { Movies: "" } });

// Copia el array 'movies_ref' a un nuevo campo 'Movies'
db.facturas.updateMany({}, [{ $set: { Movies: "$movies_ref" } }]);

// Elimina el campo 'movies_ref' de todos los documentos
db.facturas.updateMany({}, { $unset: { movies_ref: "" } });

// Array de Series en facturas

db.facturas.updateMany({}, [
    {
        $set: {
            series_ref: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        serie_id: {
                            $concat: [
                                "$$serie.title",
                                "/",
                                { $toString: "$$serie.total_seasons" },
                            ],
                        },
                        License: "$$serie.License",
                        season: "$$serie.season",
                        episode: "$$serie.episode",
                        viewing_pct: "$$serie.viewing_pct",
                        date_time: "$$serie.date_time",
                    },
                },
            },
        },
    },
]);

// Crea Series con todos los valores de temporadas correctamente

db.facturas.aggregate([
    { $unwind: "$Series" },
    {
        $group: {
            _id: {
                $concat: [
                    "$Series.title",
                    "/",
                    { $toString: "$Series.total_seasons" },
                ],
            },
            title: { $first: "$Series.title" },
            avg_duration: { $first: "$Series.avg_duration" },
            total_seasons: { $first: "$Series.total_seasons" },
            episodes_per_season: {
                $push: {
                    season: "$Series.season",
                    episode: "$Series.episode",
                },
            },
        },
    },
    {
        $addFields: {
            season_episodes: {
                $map: {
                    input: { $range: [1, { $add: ["$total_seasons", 1] }] },
                    as: "s",
                    in: {
                        $let: {
                            vars: {
                                eps: {
                                    $filter: {
                                        input: "$episodes_per_season",
                                        as: "ep",
                                        cond: { $eq: ["$$ep.season", "$$s"] },
                                    },
                                },
                            },
                            in: {
                                $cond: [
                                    { $gt: [{ $size: "$$eps" }, 0] },
                                    { $max: "$$eps.episode" },
                                    0,
                                ],
                            },
                        },
                    },
                },
            },
        },
    },
    {
        $project: {
            _id: 1,
            title: 1,
            avg_duration: 1,
            total_seasons: 1,
            season_episodes: 1,
        },
    },
    {
        $merge: {
            into: "Series",
            whenMatched: "merge",
            whenNotMatched: "insert",
        },
    },
]);

db.facturas.updateMany({}, { $unset: { Series: "" } });

// Copia los datos de series_ref a Series
db.facturas.updateMany({}, [{ $set: { Series: "$series_ref" } }]);

// Borra el campo series_ref
db.facturas.updateMany({}, { $unset: { series_ref: "" } });
