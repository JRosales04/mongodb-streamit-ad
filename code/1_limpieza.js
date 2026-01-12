// IDENTIFICADORES NULOS O DUPLICADOS

db.facturas.aggregate([
    {
        $group: {
            _id: "$_id",
            count: {
                $sum: 1,
            },
        },
    },
    {
        $match: {
            count: {
                $gt: 1,
            },
        },
    },
]);

db.facturas.find({
    _id: null,
});

// ESTANDARIZACIÓN DE NOMBRE DE ATRIBUTOS

db.facturas.updateMany(
    { "charge date": { $exists: true } },
    { $rename: { "charge date": "charge_date" } }
);
db.facturas.updateMany(
    { "Client.Birth date": { $exists: true } },
    { $rename: { "Client.Birth date": "Client.birth_date" } }
);
db.facturas.updateMany(
    { "Client.customer code": { $exists: true } },
    { $rename: { "Client.customer code": "Client.customer_code" } }
);
db.facturas.updateMany(
    { "Client.DNI": { $exists: true } },
    { $rename: { "Client.DNI": "Client.dni" } }
);
db.facturas.updateMany(
    { "Client.Email": { $exists: true } },
    { $rename: { "Client.Email": "Client.email" } }
);
db.facturas.updateMany(
    { "Client.Name": { $exists: true } },
    { $rename: { "Client.Name": "Client.name" } }
);
db.facturas.updateMany(
    { "Client.Phone": { $exists: true } },
    { $rename: { "Client.Phone": "Client.phone" } }
);
db.facturas.updateMany(
    { "Client.Surname": { $exists: true } },
    { $rename: { "Client.Surname": "Client.surname" } }
);
db.facturas.updateMany(
    { "contract.end date": { $exists: true } },
    { $rename: { "contract.end date": "contract.end_date" } }
);
db.facturas.updateMany(
    { "contract.contract ID": { $exists: true } },
    { $rename: { "contract.contract ID": "contract.contract_id" } }
);
db.facturas.updateMany(
    { "contract.product.cost per content": { $exists: true } },
    {
        $rename: {
            "contract.product.cost per content":
                "contract.product.cost_per_content",
        },
    }
);
db.facturas.updateMany(
    { "contract.product.cost per day": { $exists: true } },
    {
        $rename: {
            "contract.product.cost per day": "contract.product.cost_per_day",
        },
    }
);
db.facturas.updateMany(
    { "contract.product.cost per minute": { $exists: true } },
    {
        $rename: {
            "contract.product.cost per minute":
                "contract.product.cost_per_minute",
        },
    }
);
db.facturas.updateMany(
    { "contract.product.cost per view": { $exists: true } },
    {
        $rename: {
            "contract.product.cost per view": "contract.product.cost_per_view",
        },
    }
);
db.facturas.updateMany(
    { "contract.product.monthly fee": { $exists: true } },
    {
        $rename: {
            "contract.product.monthly fee": "contract.product.monthly_fee",
        },
    }
);
db.facturas.updateMany(
    { "contract.product.Reference": { $exists: true } },
    { $rename: { "contract.product.Reference": "contract.product.reference" } }
);
db.facturas.updateMany(
    { "contract.start date": { $exists: true } },
    { $rename: { "contract.start date": "contract.start_date" } }
);
db.facturas.updateMany(
    { "contract.ZIP": { $exists: true } },
    { $rename: { "contract.ZIP": "contract.zip" } }
);
db.facturas.updateMany(
    { "dump date": { $exists: true } },
    { $rename: { "dump date": "dump_date" } }
);

db.facturas.updateMany({ "Movies.Date": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: ["$$movie", { date: "$$movie.Date" }],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Date" },
]);
db.facturas.updateMany({ "Movies.Details.Aspect ratio": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            aspect_ratio:
                                                "$$movie.Details.Aspect ratio",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Aspect ratio" },
]);
db.facturas.updateMany({ "Movies.Details.Budget": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        { budget: "$$movie.Details.Budget" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Budget" },
]);
db.facturas.updateMany(
    { "Movies.Details.Cast.Facebook likes": { $exists: true } },
    [
        {
            $set: {
                Movies: {
                    $map: {
                        input: "$Movies",
                        as: "movie",
                        in: {
                            $mergeObjects: [
                                "$$movie",
                                {
                                    Details: {
                                        $mergeObjects: [
                                            "$$movie.Details",
                                            {
                                                Cast: {
                                                    $mergeObjects: [
                                                        "$$movie.Details.Cast",
                                                        {
                                                            facebook_likes:
                                                                "$$movie.Details.Cast.Facebook likes",
                                                        },
                                                    ],
                                                },
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                },
            },
        },
        { $unset: "Movies.Details.Cast.Facebook likes" },
    ]
);
db.facturas.updateMany(
    { "Movies.Details.Cast.Stars.Facebook likes": { $exists: true } },
    [
        {
            $set: {
                Movies: {
                    $map: {
                        input: "$Movies",
                        as: "movie",
                        in: {
                            $mergeObjects: [
                                "$$movie",
                                {
                                    Details: {
                                        $mergeObjects: [
                                            "$$movie.Details",
                                            {
                                                Cast: {
                                                    $mergeObjects: [
                                                        "$$movie.Details.Cast",
                                                        {
                                                            Stars: {
                                                                $map: {
                                                                    input: "$$movie.Details.Cast.Stars",
                                                                    as: "star",
                                                                    in: {
                                                                        $mergeObjects:
                                                                            [
                                                                                "$$star",
                                                                                {
                                                                                    facebook_likes:
                                                                                        "$$star.Facebook likes",
                                                                                },
                                                                            ],
                                                                    },
                                                                },
                                                            },
                                                        },
                                                    ],
                                                },
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                },
            },
        },
        { $unset: "Movies.Details.Cast.Stars.Facebook likes" },
    ]
);
db.facturas.updateMany(
    { "Movies.Details.Cast.Stars.Player": { $exists: true } },
    [
        {
            $set: {
                Movies: {
                    $map: {
                        input: "$Movies",
                        as: "movie",
                        in: {
                            $mergeObjects: [
                                "$$movie",
                                {
                                    Details: {
                                        $mergeObjects: [
                                            "$$movie.Details",
                                            {
                                                Cast: {
                                                    $mergeObjects: [
                                                        "$$movie.Details.Cast",
                                                        {
                                                            Stars: {
                                                                $map: {
                                                                    input: "$$movie.Details.Cast.Stars",
                                                                    as: "star",
                                                                    in: {
                                                                        $mergeObjects:
                                                                            [
                                                                                "$$star",
                                                                                {
                                                                                    player: "$$star.Player",
                                                                                },
                                                                            ],
                                                                    },
                                                                },
                                                            },
                                                        },
                                                    ],
                                                },
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                },
            },
        },
        { $unset: "Movies.Details.Cast.Stars.Player" },
    ]
);
db.facturas.updateMany({ "Movies.Details.Color": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        { color: "$$movie.Details.Color" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Color" },
]);
db.facturas.updateMany({ "Movies.Details.Content Rating": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            content_rating:
                                                "$$movie.Details.Content Rating",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Content Rating" },
]);
db.facturas.updateMany({ "Movies.Details.Country": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        { country: "$$movie.Details.Country" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Country" },
]);
db.facturas.updateMany({ "Movies.Details.Critic reviews": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            critic_reviews:
                                                "$$movie.Details.Critic reviews",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Critic reviews" },
]);
db.facturas.updateMany(
    { "Movies.Details.Director.Facebook likes": { $exists: true } },
    [
        {
            $set: {
                Movies: {
                    $map: {
                        input: "$Movies",
                        as: "movie",
                        in: {
                            $mergeObjects: [
                                "$$movie",
                                {
                                    Details: {
                                        $mergeObjects: [
                                            "$$movie.Details",
                                            {
                                                Director: {
                                                    $mergeObjects: [
                                                        "$$movie.Details.Director",
                                                        {
                                                            facebook_likes:
                                                                "$$movie.Details.Director.Facebook likes",
                                                        },
                                                    ],
                                                },
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                },
            },
        },
        { $unset: "Movies.Details.Director.Facebook likes" },
    ]
);
db.facturas.updateMany({ "Movies.Details.Director.Name": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            Director: {
                                                $mergeObjects: [
                                                    "$$movie.Details.Director",
                                                    {
                                                        name: "$$movie.Details.Director.Name",
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Director.Name" },
]);
db.facturas.updateMany({ "Movies.Details.Duration": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            duration:
                                                "$$movie.Details.Duration",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Duration" },
]);
db.facturas.updateMany({ "Movies.Details.Facebook likes": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            facebook_likes:
                                                "$$movie.Details.Facebook likes",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Facebook likes" },
]);
db.facturas.updateMany(
    { "Movies.Details.Faces in poster": { $exists: true } },
    [
        {
            $set: {
                Movies: {
                    $map: {
                        input: "$Movies",
                        as: "movie",
                        in: {
                            $mergeObjects: [
                                "$$movie",
                                {
                                    Details: {
                                        $mergeObjects: [
                                            "$$movie.Details",
                                            {
                                                faces_in_poster:
                                                    "$$movie.Details.Faces in poster",
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                },
            },
        },
        { $unset: "Movies.Details.Faces in poster" },
    ]
);
db.facturas.updateMany({ "Movies.Details.Genres": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        { genres: "$$movie.Details.Genres" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Genres" },
]);
db.facturas.updateMany({ "Movies.Details.Gross": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        { gross: "$$movie.Details.Gross" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Gross" },
]);
db.facturas.updateMany({ "Movies.Details.IMDB link": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            imdb_link:
                                                "$$movie.Details.IMDB link",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.IMDB link" },
]);
db.facturas.updateMany({ "Movies.Details.IMDB score": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            imdb_score:
                                                "$$movie.Details.IMDB score",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.IMDB score" },
]);
db.facturas.updateMany({ "Movies.Details.Keywords": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            keywords:
                                                "$$movie.Details.Keywords",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Keywords" },
]);
db.facturas.updateMany({ "Movies.Details.Language": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            language:
                                                "$$movie.Details.Language",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Language" },
]);
db.facturas.updateMany({ "Movies.Details.User reviews": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            user_reviews:
                                                "$$movie.Details.User reviews",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.User reviews" },
]);
db.facturas.updateMany({ "Movies.Details.Voted users": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            voted_users:
                                                "$$movie.Details.Voted users",
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Voted users" },
]);
db.facturas.updateMany({ "Movies.Details.Year": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        { year: "$$movie.Details.Year" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Details.Year" },
]);
db.facturas.updateMany({ "Movies.License.Date": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                License: {
                                    $mergeObjects: [
                                        "$$movie.License",
                                        { date: "$$movie.License.Date" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.License.Date" },
]);
db.facturas.updateMany({ "Movies.License.Time": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                License: {
                                    $mergeObjects: [
                                        "$$movie.License",
                                        { time: "$$movie.License.Time" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.License.Time" },
]);
db.facturas.updateMany({ "Movies.Time": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: ["$$movie", { time: "$$movie.Time" }],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Time" },
]);
db.facturas.updateMany({ "Movies.Viewing PCT": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            { viewing_pct: "$$movie.Viewing PCT" },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Viewing PCT" },
]);
db.facturas.updateMany({ "Series.Avg duration": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            { avg_duration: "$$serie.Avg duration" },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Series.Avg duration" },
]);
db.facturas.updateMany({ "Series.Date": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: ["$$serie", { date: "$$serie.Date" }],
                    },
                },
            },
        },
    },
    { $unset: "Series.Date" },
]);
db.facturas.updateMany({ "Series.Episode": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            { episode: "$$serie.Episode" },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Series.Episode" },
]);
db.facturas.updateMany({ "Series.License.Date": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            {
                                License: {
                                    $mergeObjects: [
                                        "$$serie.License",
                                        { date: "$$serie.License.Date" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Series.License.Date" },
]);
db.facturas.updateMany({ "Series.License.Time": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            {
                                License: {
                                    $mergeObjects: [
                                        "$$serie.License",
                                        { time: "$$serie.License.Time" },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Series.License.Time" },
]);
db.facturas.updateMany({ "Series.Season": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            { season: "$$serie.Season" },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Series.Season" },
]);
db.facturas.updateMany({ "Series.Time": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: ["$$serie", { time: "$$serie.Time" }],
                    },
                },
            },
        },
    },
    { $unset: "Series.Time" },
]);
db.facturas.updateMany({ "Series.Title": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: ["$$serie", { title: "$$serie.Title" }],
                    },
                },
            },
        },
    },
    { $unset: "Series.Title" },
]);
db.facturas.updateMany({ "Series.Total Episodes": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            { total_episodes: "$$serie.Total Episodes" },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Series.Total Episodes" },
]);
db.facturas.updateMany({ "Series.Total Seasons": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            { total_seasons: "$$serie.Total Seasons" },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Series.Total Seasons" },
]);
db.facturas.updateMany({ "Series.Viewing PCT": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            { viewing_pct: "$$serie.Viewing PCT" },
                        ],
                    },
                },
            },
        },
    },
    { $unset: "Series.Viewing PCT" },
]);
db.facturas.updateMany({ "Movies.Title": { $exists: true } }, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: ["$$movie", { title: "$$movie.Title" }],
                    },
                },
            },
        },
    },
    { $unset: "Movies.Title" },
]);

db.facturas.updateMany({ contract: { $exists: true } }, [
    { $set: { Contract: "$contract" } },
    { $unset: "contract" },
]);
db.facturas.updateMany({ "Contract.product": { $exists: true } }, [
    { $set: { "Contract.Product": "$Contract.product" } },
    { $unset: "Contract.product" },
]);

// LIMPIEZA DE ATRIBUTOS

db.facturas.updateMany({ billing: { $type: "string" } }, [
    {
        $set: {
            billing: {
                $dateFromString: {
                    dateString: {
                        $concat: ["01 ", "$billing"],
                    },
                    format: "%d %B %Y",
                },
            },
        },
    },
]);

db.facturas.updateMany({ charge_date: { $type: "string" } }, [
    {
        $set: {
            charge_date: {
                $dateFromString: {
                    dateString: {
                        $let: {
                            vars: {
                                parts: { $split: ["$charge_date", "/"] },
                            },
                            in: {
                                $concat: [
                                    { $arrayElemAt: ["$$parts", 0] },
                                    "/",
                                    { $arrayElemAt: ["$$parts", 1] },
                                    "/",
                                    {
                                        $concat: [
                                            "20",
                                            { $arrayElemAt: ["$$parts", 2] },
                                        ],
                                    },
                                ],
                            },
                        },
                    },
                    format: "%d/%m/%Y",
                    timezone: "UTC",
                },
            },
        },
    },
]);

db.facturas.updateMany({ dump_date: { $type: "string" } }, [
    {
        $set: {
            dump_date: {
                $cond: [
                    {
                        $or: [
                            { $eq: ["$dump_date", null] },
                            { $eq: ["$dump_date", ""] },
                        ],
                    },
                    "$dump_date", // deja nulos o vacíos igual
                    {
                        $dateFromString: {
                            dateString: {
                                $let: {
                                    vars: {
                                        parts: { $split: ["$dump_date", "/"] },
                                        yy: {
                                            $toInt: {
                                                $arrayElemAt: [
                                                    {
                                                        $split: [
                                                            "$dump_date",
                                                            "/",
                                                        ],
                                                    },
                                                    2,
                                                ],
                                            },
                                        },
                                    },
                                    in: {
                                        $concat: [
                                            { $arrayElemAt: ["$$parts", 0] },
                                            "/", // día
                                            { $arrayElemAt: ["$$parts", 1] },
                                            "/", // mes
                                            {
                                                $cond: [
                                                    { $lt: ["$$yy", 25] }, // 00–24 → 2000–2024
                                                    {
                                                        $concat: [
                                                            "20",
                                                            {
                                                                $arrayElemAt: [
                                                                    "$$parts",
                                                                    2,
                                                                ],
                                                            },
                                                        ],
                                                    },
                                                    {
                                                        $concat: [
                                                            "19",
                                                            {
                                                                $arrayElemAt: [
                                                                    "$$parts",
                                                                    2,
                                                                ],
                                                            },
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                },
                            },
                            format: "%d/%m/%Y",
                            timezone: "UTC",
                        },
                    },
                ],
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            TOTAL: {
                $toDecimal: "$TOTAL",
            },
        },
    },
]);

db.facturas.updateMany({ "Client.birth_date": { $type: "string" } }, [
    {
        $set: {
            "Client.birth_date": {
                $dateFromString: {
                    dateString: {
                        $let: {
                            vars: {
                                parts: { $split: ["$Client.birth_date", "/"] },
                                yy: {
                                    $toInt: {
                                        $arrayElemAt: [
                                            {
                                                $split: [
                                                    "$Client.birth_date",
                                                    "/",
                                                ],
                                            },
                                            2,
                                        ],
                                    },
                                },
                            },
                            in: {
                                $concat: [
                                    { $arrayElemAt: ["$$parts", 0] },
                                    "/",
                                    { $arrayElemAt: ["$$parts", 1] },
                                    "/",
                                    {
                                        $cond: [
                                            { $lt: ["$$yy", 25] },
                                            {
                                                $concat: [
                                                    "20",
                                                    {
                                                        $arrayElemAt: [
                                                            "$$parts",
                                                            2,
                                                        ],
                                                    },
                                                ],
                                            },
                                            {
                                                $concat: [
                                                    "19",
                                                    {
                                                        $arrayElemAt: [
                                                            "$$parts",
                                                            2,
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                        },
                    },
                    format: "%d/%m/%Y",
                    timezone: "UTC",
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            "Client.surname": {
                $cond: {
                    if: { $eq: [{ $type: "$Client.surname" }, "string"] },
                    then: { $split: ["$Client.surname", " "] },
                    else: "$Client.surname",
                },
            },
        },
    },
]);

db.facturas.updateMany({ "Contract.end_date": { $type: "string" } }, [
    {
        $set: {
            "Contract.end_date": {
                $cond: [
                    {
                        $or: [
                            { $eq: ["$Contract.end_date", null] },
                            { $eq: ["$Contract.end_date", ""] },
                        ],
                    },
                    "$Contract.end_date", // deja los nulos/vacíos igual
                    {
                        $dateFromString: {
                            dateString: {
                                $let: {
                                    vars: {
                                        parts: {
                                            $split: ["$Contract.end_date", "/"],
                                        },
                                        yy: {
                                            $toInt: {
                                                $arrayElemAt: [
                                                    {
                                                        $split: [
                                                            "$Contract.end_date",
                                                            "/",
                                                        ],
                                                    },
                                                    2,
                                                ],
                                            },
                                        },
                                    },
                                    in: {
                                        $concat: [
                                            { $arrayElemAt: ["$$parts", 0] },
                                            "/",
                                            { $arrayElemAt: ["$$parts", 1] },
                                            "/",
                                            {
                                                $cond: [
                                                    { $lt: ["$$yy", 25] },
                                                    {
                                                        $concat: [
                                                            "20",
                                                            {
                                                                $arrayElemAt: [
                                                                    "$$parts",
                                                                    2,
                                                                ],
                                                            },
                                                        ],
                                                    },
                                                    {
                                                        $concat: [
                                                            "19",
                                                            {
                                                                $arrayElemAt: [
                                                                    "$$parts",
                                                                    2,
                                                                ],
                                                            },
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                },
                            },
                            format: "%d/%m/%Y",
                            timezone: "UTC",
                        },
                    },
                ],
            },
        },
    },
]);

db.facturas.updateMany({ "Contract.start_date": { $type: "string" } }, [
    {
        $set: {
            "Contract.start_date": {
                $cond: [
                    {
                        $or: [
                            { $eq: ["$Contract.start_date", null] },
                            { $eq: ["$Contract.start_date", ""] },
                        ],
                    },
                    "$Contract.start_date", // deja nulos o vacíos igual
                    {
                        $dateFromString: {
                            dateString: {
                                $let: {
                                    vars: {
                                        parts: {
                                            $split: [
                                                "$Contract.start_date",
                                                "/",
                                            ],
                                        },
                                        yy: {
                                            $toInt: {
                                                $arrayElemAt: [
                                                    {
                                                        $split: [
                                                            "$Contract.start_date",
                                                            "/",
                                                        ],
                                                    },
                                                    2,
                                                ],
                                            },
                                        },
                                    },
                                    in: {
                                        $concat: [
                                            { $arrayElemAt: ["$$parts", 0] },
                                            "/",
                                            { $arrayElemAt: ["$$parts", 1] },
                                            "/",
                                            {
                                                $cond: [
                                                    { $lt: ["$$yy", 25] }, // 00–24 → 2000–2024
                                                    {
                                                        $concat: [
                                                            "20",
                                                            {
                                                                $arrayElemAt: [
                                                                    "$$parts",
                                                                    2,
                                                                ],
                                                            },
                                                        ],
                                                    },
                                                    {
                                                        $concat: [
                                                            "19",
                                                            {
                                                                $arrayElemAt: [
                                                                    "$$parts",
                                                                    2,
                                                                ],
                                                            },
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                },
                            },
                            format: "%d/%m/%Y",
                            timezone: "UTC",
                        },
                    },
                ],
            },
        },
    },
]);

db.facturas.updateMany({ "Contract.zip": { $type: "string" } }, [
    {
        $set: {
            "Contract.zip": {
                $cond: [
                    {
                        $or: [
                            { $eq: ["$Contract.zip", null] },
                            { $eq: ["$Contract.zip", ""] },
                            {
                                $regexMatch: {
                                    input: "$Contract.zip",
                                    regex: /[^0-9]/,
                                },
                            },
                        ],
                    },
                    "$Contract.zip", // deja igual si no es numérico o es nulo/vacío
                    { $toInt: "$Contract.zip" }, // convierte a int32
                ],
            },
        },
    },
]);

db.facturas.updateMany(
    { "Contract.Product.cost_per_content": { $type: "double" } },
    [
        {
            $set: {
                "Contract.Product.cost_per_content": {
                    $toDecimal: "$Contract.Product.cost_per_content",
                },
            },
        },
    ]
);

db.facturas.updateMany(
    {
        "Contract.Product.cost_per_day": {
            $exists: true,
        },
    },
    [
        {
            $set: {
                "Contract.Product.cost_per_day": {
                    $toDecimal: "$Contract.Product.cost_per_day",
                },
            },
        },
    ]
);

db.facturas.updateMany(
    {
        "Contract.Product.cost_per_minute": { $exists: true },
    },
    [
        {
            $set: {
                "Contract.Product.cost_per_minute": {
                    $toDecimal: "$Contract.Product.cost_per_minute",
                },
            },
        },
    ]
);

db.facturas.updateMany(
    {
        "Contract.Product.cost_per_view": { $exists: true },
    },
    [
        {
            $set: {
                "Contract.Product.cost_per_view": {
                    $toDecimal: "$Contract.Product.cost_per_view",
                },
            },
        },
    ]
);

db.facturas.updateMany(
    {
        "Contract.Product.monthly_fee": { $exists: true },
    },
    [
        {
            $set: {
                "Contract.Product.monthly_fee": {
                    $toDecimal: "$Contract.Product.monthly_fee",
                },
            },
        },
    ]
);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                date_time: {
                                    $cond: [
                                        {
                                            $and: [
                                                { $ne: ["$$m.date", null] },
                                                { $ne: ["$$m.time", null] },
                                                { $ne: ["$$m.date", ""] },
                                                { $ne: ["$$m.time", ""] },
                                            ],
                                        },
                                        {
                                            $concat: [
                                                "$$m.date",
                                                " ",
                                                "$$m.time",
                                            ],
                                        },
                                        null,
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                date_time: {
                                    $cond: [
                                        { $ne: ["$$m.date_time", null] },
                                        {
                                            $dateFromString: {
                                                dateString: "$$m.date_time",
                                                format: "%d/%m/%Y %H:%M:%S",
                                            },
                                        },
                                        null,
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $arrayToObject: {
                            $filter: {
                                input: { $objectToArray: "$$m" },
                                as: "field",
                                cond: {
                                    $and: [
                                        { $ne: ["$$field.k", "date"] },
                                        { $ne: ["$$field.k", "time"] },
                                    ],
                                },
                            },
                        },
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                viewing_pct: {
                                    $cond: [
                                        { $ne: ["$$m.viewing_pct", null] },
                                        { $toDouble: "$$m.viewing_pct" },
                                        null,
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            aspect_ratio: {
                                                $cond: [
                                                    {
                                                        $ifNull: [
                                                            "$$m.Details.aspect_ratio",
                                                            false,
                                                        ],
                                                    },
                                                    {
                                                        $toDouble:
                                                            "$$m.Details.aspect_ratio",
                                                    },
                                                    null,
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            budget: {
                                                $cond: [
                                                    {
                                                        $eq: [
                                                            "$$m.Details.budget",
                                                            undefined,
                                                        ],
                                                    },
                                                    null,
                                                    {
                                                        $toDecimal:
                                                            "$$m.Details.budget",
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            content_rating: {
                                                $ifNull: [
                                                    "$$m.Details.content_rating",
                                                    "Not Rated",
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            Cast: {
                                                $mergeObjects: [
                                                    "$$m.Details.Cast",
                                                    {
                                                        Stars: {
                                                            $map: {
                                                                input: "$$m.Details.Cast.Stars",
                                                                as: "s",
                                                                in: {
                                                                    $mergeObjects:
                                                                        [
                                                                            "$$s",
                                                                            {
                                                                                facebook_likes:
                                                                                    {
                                                                                        $toInt: {
                                                                                            $ifNull:
                                                                                                [
                                                                                                    "$$s.facebook_likes",
                                                                                                    0,
                                                                                                ],
                                                                                        },
                                                                                    },
                                                                            },
                                                                        ],
                                                                },
                                                            },
                                                        },
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            Cast: {
                                                $mergeObjects: [
                                                    "$$m.Details.Cast",
                                                    {
                                                        facebook_likes: {
                                                            $toInt: {
                                                                $ifNull: [
                                                                    "$$m.Details.Cast.facebook_likes",
                                                                    0,
                                                                ],
                                                            },
                                                        },
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            critic_reviews: {
                                                $toInt: {
                                                    $ifNull: [
                                                        "$$m.Details.critic_reviews",
                                                        0,
                                                    ],
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            Director: {
                                                $mergeObjects: [
                                                    "$$m.Details.Director",
                                                    {
                                                        facebook_likes: {
                                                            $toInt: {
                                                                $ifNull: [
                                                                    "$$m.Details.Director.facebook_likes",
                                                                    0,
                                                                ],
                                                            },
                                                        },
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            facebook_likes: {
                                                $toInt: {
                                                    $ifNull: [
                                                        "$$m.Details.facebook_likes",
                                                        0,
                                                    ],
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            faces_in_poster: {
                                                $toInt: {
                                                    $ifNull: [
                                                        "$$m.Details.faces_in_poster",
                                                        0,
                                                    ],
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            gross: {
                                                $cond: {
                                                    if: {
                                                        $eq: [
                                                            {
                                                                $type: "$$m.Details.gross",
                                                            },
                                                            "int",
                                                        ],
                                                    },
                                                    then: {
                                                        $toDecimal:
                                                            "$$m.Details.gross",
                                                    },
                                                    else: "$$m.Details.gross",
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            imdb_score: {
                                                $toDouble:
                                                    "$$m.Details.imdb_score",
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                License: {
                                    $mergeObjects: [
                                        "$$m.License",
                                        {
                                            date_time: {
                                                $concat: [
                                                    "$$m.License.date",
                                                    " ",
                                                    "$$m.License.time",
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                License: {
                                    $mergeObjects: [
                                        "$$m.License",
                                        {
                                            date_time: {
                                                $dateFromString: {
                                                    dateString:
                                                        "$$m.License.date_time",
                                                    format: "%d/%m/%Y %H:%M:%S",
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: ["Movies.License.date", "Movies.License.time"] },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "s",
                    in: {
                        $mergeObjects: [
                            "$$s",
                            {
                                date_time: {
                                    $dateFromString: {
                                        dateString: {
                                            $concat: [
                                                "$$s.date",
                                                " ",
                                                "$$s.time",
                                            ],
                                        },
                                        format: "%d/%m/%Y %H:%M:%S",
                                    },
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
    { $unset: ["Series.date", "Series.time"] },
]);

db.facturas.updateMany({ "Series.viewing_pct": { $type: "int" } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "s",
                    in: {
                        $mergeObjects: [
                            "$$s",
                            {
                                viewing_pct: {
                                    $cond: {
                                        if: {
                                            $eq: [
                                                { $type: "$$s.viewing_pct" },
                                                "int",
                                            ],
                                        },
                                        then: { $toDouble: "$$s.viewing_pct" },
                                        else: "$$s.viewing_pct",
                                    },
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany(
    {
        "Series.License.date": { $exists: true },
        "Series.License.time": { $exists: true },
    },
    [
        {
            $set: {
                Series: {
                    $map: {
                        input: "$Series",
                        as: "serie",
                        in: {
                            $mergeObjects: [
                                "$$serie",
                                {
                                    License: {
                                        $mergeObjects: [
                                            "$$serie.License",
                                            {
                                                date_time: {
                                                    $concat: [
                                                        "$$serie.License.date",
                                                        " ",
                                                        "$$serie.License.time",
                                                    ],
                                                },
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                },
            },
        },
    ]
);

db.facturas.updateMany({ "Series.License.date_time": { $exists: true } }, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            {
                                License: {
                                    $mergeObjects: [
                                        "$$serie.License",
                                        {
                                            date_time: {
                                                $dateFromString: {
                                                    dateString:
                                                        "$$serie.License.date_time",
                                                    format: "%d/%m/%Y %H:%M:%S",
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            {
                                License: {
                                    $arrayToObject: {
                                        $filter: {
                                            input: {
                                                $objectToArray:
                                                    "$$serie.License",
                                            },
                                            as: "item",
                                            cond: {
                                                $and: [
                                                    {
                                                        $ne: [
                                                            "$$item.k",
                                                            "date",
                                                        ],
                                                    },
                                                    {
                                                        $ne: [
                                                            "$$item.k",
                                                            "time",
                                                        ],
                                                    },
                                                ],
                                            },
                                        },
                                    },
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

// TRATAMIENTO DE VALORES NULOS

db.facturas.updateMany({}, [
    {
        $set: {
            "Contract.Product.cost_per_view": {
                $ifNull: ["$Contract.Product.cost_per_view", null],
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            keywords: {
                                                $ifNull: [
                                                    "$$m.Details.keywords",
                                                    null,
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            language: {
                                                $ifNull: [
                                                    "$$m.Details.language",
                                                    null,
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            user_reviews: {
                                                $ifNull: [
                                                    "$$m.Details.user_reviews",
                                                    null,
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            year: {
                                                $ifNull: [
                                                    "$$m.Details.year",
                                                    null,
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            imdb_link: {
                                                $ifNull: [
                                                    "$$m.Details.imdb_link",
                                                    null,
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            TOTAL: {
                $toDecimal: "$TOTAL",
            },
        },
    },
]);

db.facturas.updateMany(
    {
        "Client.phone": { $exists: false },
    },
    {
        $set: {
            "Client.phone": null,
        },
    }
);

db.facturas.updateMany(
    {
        "Contract.end_date": { $exists: false },
    },
    {
        $set: {
            "Contract.end_date": null,
        },
    }
);

db.facturas.updateMany(
    {
        "Contract.Product.cost_per_content": { $exists: false },
    },
    {
        $set: {
            "Contract.Product.cost_per_content": null,
        },
    }
);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            budget: {
                                                $cond: [
                                                    {
                                                        $ifNull: [
                                                            "$$m.Details.budget",
                                                            false,
                                                        ],
                                                    },
                                                    "$$m.Details.budget",
                                                    null,
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            country: {
                                                $ifNull: [
                                                    "$$m.Details.country",
                                                    null,
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "m",
                    in: {
                        $mergeObjects: [
                            "$$m",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$m.Details",
                                        {
                                            gross: {
                                                $ifNull: [
                                                    "$$m.Details.gross",
                                                    null,
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);
// HOMOGENIZACION DE STRINGS (MAYUS SIN TILDES)

db.facturas.updateMany({ "Client.email": { $type: "string" } }, [
    {
        $set: {
            "Client.email": {
                $toLower: {
                    $replaceAll: {
                        input: {
                            $replaceAll: {
                                input: {
                                    $replaceAll: {
                                        input: {
                                            $replaceAll: {
                                                input: {
                                                    $replaceAll: {
                                                        input: "$Client.email",
                                                        find: "á",
                                                        replacement: "a",
                                                    },
                                                },
                                                find: "é",
                                                replacement: "e",
                                            },
                                        },
                                        find: "í",
                                        replacement: "i",
                                    },
                                },
                                find: "ó",
                                replacement: "o",
                            },
                        },
                        find: "ú",
                        replacement: "u",
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({ "Client.name": { $type: "string" } }, [
    {
        $set: {
            "Client.name": {
                $toUpper: {
                    $replaceAll: {
                        input: {
                            $replaceAll: {
                                input: {
                                    $replaceAll: {
                                        input: {
                                            $replaceAll: {
                                                input: {
                                                    $replaceAll: {
                                                        input: "$Client.name",
                                                        find: "á",
                                                        replacement: "a",
                                                    },
                                                },
                                                find: "é",
                                                replacement: "e",
                                            },
                                        },
                                        find: "í",
                                        replacement: "i",
                                    },
                                },
                                find: "ó",
                                replacement: "o",
                            },
                        },
                        find: "ú",
                        replacement: "u",
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({ "Contract.address": { $type: "string" } }, [
    {
        $set: {
            "Contract.address": {
                $toUpper: {
                    $replaceAll: {
                        input: {
                            $replaceAll: {
                                input: {
                                    $replaceAll: {
                                        input: {
                                            $replaceAll: {
                                                input: {
                                                    $replaceAll: {
                                                        input: "$Contract.address",
                                                        find: "á",
                                                        replacement: "a",
                                                    },
                                                },
                                                find: "é",
                                                replacement: "e",
                                            },
                                        },
                                        find: "í",
                                        replacement: "i",
                                    },
                                },
                                find: "ó",
                                replacement: "o",
                            },
                        },
                        find: "ú",
                        replacement: "u",
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({ "Contract.country": { $type: "string" } }, [
    {
        $set: {
            "Contract.country": {
                $toUpper: {
                    $replaceAll: {
                        input: {
                            $replaceAll: {
                                input: {
                                    $replaceAll: {
                                        input: {
                                            $replaceAll: {
                                                input: {
                                                    $replaceAll: {
                                                        input: "$Contract.country",
                                                        find: "á",
                                                        replacement: "a",
                                                    },
                                                },
                                                find: "é",
                                                replacement: "e",
                                            },
                                        },
                                        find: "í",
                                        replacement: "i",
                                    },
                                },
                                find: "ó",
                                replacement: "o",
                            },
                        },
                        find: "ú",
                        replacement: "u",
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({ "Contract.town": { $type: "string" } }, [
    {
        $set: {
            "Contract.town": {
                $toUpper: {
                    $replaceAll: {
                        input: {
                            $replaceAll: {
                                input: {
                                    $replaceAll: {
                                        input: {
                                            $replaceAll: {
                                                input: {
                                                    $replaceAll: {
                                                        input: "$Contract.town",
                                                        find: "á",
                                                        replacement: "a",
                                                    },
                                                },
                                                find: "é",
                                                replacement: "e",
                                            },
                                        },
                                        find: "í",
                                        replacement: "i",
                                    },
                                },
                                find: "ó",
                                replacement: "o",
                            },
                        },
                        find: "ú",
                        replacement: "u",
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({ "Contract.Product.reference": { $type: "string" } }, [
    {
        $set: {
            "Contract.Product.reference": {
                $toUpper: {
                    $replaceAll: {
                        input: {
                            $replaceAll: {
                                input: {
                                    $replaceAll: {
                                        input: {
                                            $replaceAll: {
                                                input: {
                                                    $replaceAll: {
                                                        input: "$Contract.Product.reference",
                                                        find: "á",
                                                        replacement: "a",
                                                    },
                                                },
                                                find: "é",
                                                replacement: "e",
                                            },
                                        },
                                        find: "í",
                                        replacement: "i",
                                    },
                                },
                                find: "ó",
                                replacement: "o",
                            },
                        },
                        find: "ú",
                        replacement: "u",
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({ "Contract.Product.type": { $type: "string" } }, [
    {
        $set: {
            "Contract.Product.type": {
                $toUpper: {
                    $replaceAll: {
                        input: {
                            $replaceAll: {
                                input: {
                                    $replaceAll: {
                                        input: {
                                            $replaceAll: {
                                                input: {
                                                    $replaceAll: {
                                                        input: "$Contract.Product.type",
                                                        find: "á",
                                                        replacement: "a",
                                                    },
                                                },
                                                find: "é",
                                                replacement: "e",
                                            },
                                        },
                                        find: "í",
                                        replacement: "i",
                                    },
                                },
                                find: "ó",
                                replacement: "o",
                            },
                        },
                        find: "ú",
                        replacement: "u",
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                title: {
                                    $toUpper: {
                                        $replaceAll: {
                                            input: {
                                                $replaceAll: {
                                                    input: {
                                                        $replaceAll: {
                                                            input: {
                                                                $replaceAll: {
                                                                    input: {
                                                                        $replaceAll:
                                                                            {
                                                                                input: "$$movie.title",
                                                                                find: "á",
                                                                                replacement:
                                                                                    "a",
                                                                            },
                                                                    },
                                                                    find: "é",
                                                                    replacement:
                                                                        "e",
                                                                },
                                                            },
                                                            find: "í",
                                                            replacement: "i",
                                                        },
                                                    },
                                                    find: "ó",
                                                    replacement: "o",
                                                },
                                            },
                                            find: "ú",
                                            replacement: "u",
                                        },
                                    },
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            color: {
                                                $toUpper: {
                                                    $replaceAll: {
                                                        input: {
                                                            $replaceAll: {
                                                                input: {
                                                                    $replaceAll:
                                                                        {
                                                                            input: {
                                                                                $replaceAll:
                                                                                    {
                                                                                        input: "$$movie.Details.color",
                                                                                        find: "á",
                                                                                        replacement:
                                                                                            "a",
                                                                                    },
                                                                            },
                                                                            find: "é",
                                                                            replacement:
                                                                                "e",
                                                                        },
                                                                },
                                                                find: "í",
                                                                replacement:
                                                                    "i",
                                                            },
                                                        },
                                                        find: "ó",
                                                        replacement: "o",
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            content_rating: {
                                                $toUpper: {
                                                    $replaceAll: {
                                                        input: {
                                                            $replaceAll: {
                                                                input: {
                                                                    $replaceAll:
                                                                        {
                                                                            input: {
                                                                                $replaceAll:
                                                                                    {
                                                                                        input: "$$movie.Details.content_rating",
                                                                                        find: "á",
                                                                                        replacement:
                                                                                            "a",
                                                                                    },
                                                                            },
                                                                            find: "é",
                                                                            replacement:
                                                                                "e",
                                                                        },
                                                                },
                                                                find: "í",
                                                                replacement:
                                                                    "i",
                                                            },
                                                        },
                                                        find: "ó",
                                                        replacement: "o",
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            country: {
                                                $toUpper: {
                                                    $replaceAll: {
                                                        input: {
                                                            $replaceAll: {
                                                                input: {
                                                                    $replaceAll:
                                                                        {
                                                                            input: {
                                                                                $replaceAll:
                                                                                    {
                                                                                        input: "$$movie.Details.country",
                                                                                        find: "á",
                                                                                        replacement:
                                                                                            "a",
                                                                                    },
                                                                            },
                                                                            find: "é",
                                                                            replacement:
                                                                                "e",
                                                                        },
                                                                },
                                                                find: "í",
                                                                replacement:
                                                                    "i",
                                                            },
                                                        },
                                                        find: "ó",
                                                        replacement: "o",
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            imdb_link: {
                                                $toUpper: {
                                                    $replaceAll: {
                                                        input: {
                                                            $replaceAll: {
                                                                input: {
                                                                    $replaceAll:
                                                                        {
                                                                            input: {
                                                                                $replaceAll:
                                                                                    {
                                                                                        input: "$$movie.Details.imdb_link",
                                                                                        find: "á",
                                                                                        replacement:
                                                                                            "a",
                                                                                    },
                                                                            },
                                                                            find: "é",
                                                                            replacement:
                                                                                "e",
                                                                        },
                                                                },
                                                                find: "í",
                                                                replacement:
                                                                    "i",
                                                            },
                                                        },
                                                        find: "ó",
                                                        replacement: "o",
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            language: {
                                                $toUpper: {
                                                    $replaceAll: {
                                                        input: {
                                                            $replaceAll: {
                                                                input: {
                                                                    $replaceAll:
                                                                        {
                                                                            input: {
                                                                                $replaceAll:
                                                                                    {
                                                                                        input: "$$movie.Details.language",
                                                                                        find: "á",
                                                                                        replacement:
                                                                                            "a",
                                                                                    },
                                                                            },
                                                                            find: "é",
                                                                            replacement:
                                                                                "e",
                                                                        },
                                                                },
                                                                find: "í",
                                                                replacement:
                                                                    "i",
                                                            },
                                                        },
                                                        find: "ó",
                                                        replacement: "o",
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            Cast: {
                                                $mergeObjects: [
                                                    "$$movie.Details.Cast",
                                                    {
                                                        Stars: {
                                                            $map: {
                                                                input: "$$movie.Details.Cast.Stars",
                                                                as: "star",
                                                                in: {
                                                                    $mergeObjects:
                                                                        [
                                                                            "$$star",
                                                                            {
                                                                                player: {
                                                                                    $toUpper:
                                                                                        {
                                                                                            $replaceAll:
                                                                                                {
                                                                                                    input: {
                                                                                                        $replaceAll:
                                                                                                            {
                                                                                                                input: {
                                                                                                                    $replaceAll:
                                                                                                                        {
                                                                                                                            input: {
                                                                                                                                $replaceAll:
                                                                                                                                    {
                                                                                                                                        input: "$$star.player",
                                                                                                                                        find: "á",
                                                                                                                                        replacement:
                                                                                                                                            "a",
                                                                                                                                    },
                                                                                                                            },
                                                                                                                            find: "é",
                                                                                                                            replacement:
                                                                                                                                "e",
                                                                                                                        },
                                                                                                                },
                                                                                                                find: "í",
                                                                                                                replacement:
                                                                                                                    "i",
                                                                                                            },
                                                                                                    },
                                                                                                    find: "ó",
                                                                                                    replacement:
                                                                                                        "o",
                                                                                                },
                                                                                        },
                                                                                },
                                                                            },
                                                                        ],
                                                                },
                                                            },
                                                        },
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            Director: {
                                                $mergeObjects: [
                                                    "$$movie.Details.Director",
                                                    {
                                                        name: {
                                                            $toUpper: {
                                                                $replaceAll: {
                                                                    input: {
                                                                        $replaceAll:
                                                                            {
                                                                                input: {
                                                                                    $replaceAll:
                                                                                        {
                                                                                            input: {
                                                                                                $replaceAll:
                                                                                                    {
                                                                                                        input: "$$movie.Details.Director.name",
                                                                                                        find: "á",
                                                                                                        replacement:
                                                                                                            "a",
                                                                                                    },
                                                                                            },
                                                                                            find: "é",
                                                                                            replacement:
                                                                                                "e",
                                                                                        },
                                                                                },
                                                                                find: "í",
                                                                                replacement:
                                                                                    "i",
                                                                            },
                                                                    },
                                                                    find: "ó",
                                                                    replacement:
                                                                        "o",
                                                                },
                                                            },
                                                        },
                                                    },
                                                ],
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Series: {
                $map: {
                    input: "$Series",
                    as: "serie",
                    in: {
                        $mergeObjects: [
                            "$$serie",
                            {
                                title: {
                                    $toUpper: {
                                        $replaceAll: {
                                            input: {
                                                $replaceAll: {
                                                    input: {
                                                        $replaceAll: {
                                                            input: {
                                                                $replaceAll: {
                                                                    input: "$$serie.title",
                                                                    find: "á",
                                                                    replacement:
                                                                        "a",
                                                                },
                                                            },
                                                            find: "é",
                                                            replacement: "e",
                                                        },
                                                    },
                                                    find: "í",
                                                    replacement: "i",
                                                },
                                            },
                                            find: "ó",
                                            replacement: "o",
                                        },
                                    },
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            "Client.surname": {
                $map: {
                    input: "$Client.surname",
                    as: "surname",
                    in: {
                        $toUpper: {
                            $replaceAll: {
                                input: {
                                    $replaceAll: {
                                        input: {
                                            $replaceAll: {
                                                input: {
                                                    $replaceAll: {
                                                        input: {
                                                            $replaceAll: {
                                                                input: "$$surname",
                                                                find: "á",
                                                                replacement:
                                                                    "a",
                                                            },
                                                        },
                                                        find: "é",
                                                        replacement: "e",
                                                    },
                                                },
                                                find: "í",
                                                replacement: "i",
                                            },
                                        },
                                        find: "ó",
                                        replacement: "o",
                                    },
                                },
                                find: "ú",
                                replacement: "u",
                            },
                        },
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            genres: {
                                                $map: {
                                                    input: "$$movie.Details.genres",
                                                    as: "genre",
                                                    in: {
                                                        $toUpper: {
                                                            $replaceAll: {
                                                                input: {
                                                                    $replaceAll:
                                                                        {
                                                                            input: {
                                                                                $replaceAll:
                                                                                    {
                                                                                        input: {
                                                                                            $replaceAll:
                                                                                                {
                                                                                                    input: "$$genre",
                                                                                                    find: "á",
                                                                                                    replacement:
                                                                                                        "a",
                                                                                                },
                                                                                        },
                                                                                        find: "é",
                                                                                        replacement:
                                                                                            "e",
                                                                                    },
                                                                            },
                                                                            find: "í",
                                                                            replacement:
                                                                                "i",
                                                                        },
                                                                },
                                                                find: "ó",
                                                                replacement:
                                                                    "o",
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);

db.facturas.updateMany({}, [
    {
        $set: {
            Movies: {
                $map: {
                    input: "$Movies",
                    as: "movie",
                    in: {
                        $mergeObjects: [
                            "$$movie",
                            {
                                Details: {
                                    $mergeObjects: [
                                        "$$movie.Details",
                                        {
                                            keywords: {
                                                $map: {
                                                    input: "$$movie.Details.keywords",
                                                    as: "keyword",
                                                    in: {
                                                        $toUpper: {
                                                            $replaceAll: {
                                                                input: {
                                                                    $replaceAll:
                                                                        {
                                                                            input: {
                                                                                $replaceAll:
                                                                                    {
                                                                                        input: {
                                                                                            $replaceAll:
                                                                                                {
                                                                                                    input: "$$keyword",
                                                                                                    find: "á",
                                                                                                    replacement:
                                                                                                        "a",
                                                                                                },
                                                                                        },
                                                                                        find: "é",
                                                                                        replacement:
                                                                                            "e",
                                                                                    },
                                                                            },
                                                                            find: "í",
                                                                            replacement:
                                                                                "i",
                                                                        },
                                                                },
                                                                find: "ó",
                                                                replacement:
                                                                    "o",
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
        },
    },
]);
