// (Q1) CONSULTA 1 (DEVUELVE TOTAL y cada customer_code)

db.facturas.aggregate([
    { $unwind: "$Series" },
    { $match: { "Series.viewing_pct": { $gte: 95, $lte: 100 } } }, // Consideramos visto como haber visto >=95%

    // Identificador único para cliente, serie, temporada y episodio (evitar duplicados)
    {
        $group: {
            _id: {
                customer_code: "$Client.customer_code",
                serie_id: "$Series.serie_id",
                season: "$Series.season",
                episode: "$Series.episode",
            },
        },
    },

    // Contar episodios únicos por cliente, serie y temporada
    {
        $group: {
            _id: {
                customer_code: "$_id.customer_code",
                serie_id: "$_id.serie_id",
                season: "$_id.season",
            },
            uniqueEpisodes: { $sum: 1 },
        },
    },

    // Consultar información de la serie para obtener total episodios de temporada
    {
        $lookup: {
            from: "Series",
            localField: "_id.serie_id",
            foreignField: "_id",
            as: "serieInfo",
        },
    },
    { $unwind: "$serieInfo" },

    // Filtrar solo temporadas completas vistas
    {
        $match: {
            $expr: {
                $eq: [
                    "$uniqueEpisodes",
                    {
                        $arrayElemAt: [
                            "$serieInfo.season_episodes",
                            { $subtract: ["$_id.season", 1] },
                        ],
                    },
                ],
            },
        },
    },

    // Agrupar solo por cliente para evitar repetidos
    {
        $group: {
            _id: "$_id.customer_code",
        },
    },

    // Proyectar lista de clientes
    {
        $project: {
            _id: 0,
            customer_code: "$_id",
        },
    },

    // Agrupar todos para contar total serial lovers (hacer una colección final con dos campos)
    {
        $group: {
            _id: null,
            total_serial_lovers: { $sum: 1 },
            customers: { $push: "$customer_code" },
        },
    },

    // Proyectar salida final
    {
        $project: {
            _id: 0,
            total_serial_lovers: 1,
            customers: 1,
        },
    },
]);

// (Q2) CONSULTA 2 (Apellido más frecuente por país de contrato)

db.facturas.aggregate([
    // Descomponer el array de apellidos para contar individualmente cada apellido
    { $unwind: "$Client.surname" },

    // Agrupar por país y apellido contando ocurrencias
    {
        $group: {
            _id: {
                country: "$Contract.country",
                surname: "$Client.surname",
            },
            count: { $sum: 1 },
        },
    },

    // Ordenar por país y número de ocurrencias descendente
    {
        $sort: {
            "_id.country": 1,
            count: -1,
        },
    },

    // Agrupar por país para obtener sólo el apellido con mayor ocurrencia (el primero por orden)
    {
        $group: {
            _id: "$_id.country",
            mostCommonSurname: { $first: "$_id.surname" },
            count: { $first: "$count" },
        },
    },

    // Proyectar para presentar resultados claros
    {
        $project: {
            _id: 0,
            country: "$_id",
            mostCommonSurname: 1,
            count: 1,
        },
    },
]);

// (Q3) CONSULTA 3 (Actores populares)

db.facturas.aggregate([
    // Filtrar facturas con contrato en España
    { $match: { "Contract.country": "SPAIN" } },

    // Descomponer el array Movies
    { $unwind: "$Movies" },

    // Lookup para traer detalles de la película según mov_id
    {
        $lookup: {
            from: "Peliculas", // Nombre correcto con mayúscula
            localField: "Movies.mov_id",
            foreignField: "_id",
            as: "movieDetails",
        },
    },
    { $unwind: "$movieDetails" },

    // Descomponer el array de actores en la película
    { $unwind: "$movieDetails.Details.Cast.Stars" },

    // Agrupar por nombre de actor, contando las veces que aparece
    {
        $group: {
            _id: "$movieDetails.Details.Cast.Stars.player",
            timesViewed: { $sum: 1 },
        },
    },

    // Ordenar por número de visualizaciones descendente
    { $sort: { timesViewed: -1 } },

    // Limitar a los 5 actores más vistos
    { $limit: 5 },

    // Proyectar la salida
    {
        $project: {
            _id: 0,
            actor: "$_id",
            timesViewed: 1,
        },
    },
]);

// (Q4) CONSULTA 4 (top-10 anual de películas del siglo XX más vistas)

db.facturas.aggregate([
    // Descomponer las películas vistas en cada factura
    { $unwind: "$Movies" },

    // Lookup para traer detalles de la película
    {
        $lookup: {
            from: "Peliculas",
            localField: "Movies.mov_id",
            foreignField: "_id",
            as: "movieDetails",
        },
    },
    { $unwind: "$movieDetails" },

    // Filtrar películas del siglo XX (1900-1999)
    {
        $match: {
            "movieDetails.Details.year": { $gte: 1900, $lte: 1999 },
        },
    },

    // Agrupar por año y película para contar visualizaciones
    {
        $group: {
            _id: {
                year: "$movieDetails.Details.year",
                movie_id: "$movieDetails._id",
                title: "$movieDetails.title",
            },
            viewsCount: { $sum: 1 },
        },
    },

    // Ordenar por año ascendente y número de vistas descendente
    {
        $sort: {
            "_id.year": 1,
            viewsCount: -1,
        },
    },

    // Agrupar por año para preparar anidar top 10
    {
        $group: {
            _id: "$_id.year",
            movies: {
                $push: {
                    movie_id: "$_id.movie_id",
                    title: "$_id.title",
                    viewsCount: "$viewsCount",
                },
            },
        },
    },

    // Limitar el array de películas a top 10 por año
    {
        $project: {
            year: "$_id",
            topMovies: { $slice: ["$movies", 10] },
            _id: 0,
        },
    },

    // Ordenar por año ascendente (opcional)
    { $sort: { year: 1 } },
]);

// (Q5) CONSULTA 5 (total de ingresos en un mes)

db.facturas.aggregate([
    // Filtrar por facturas cuya fecha de facturación está en febrero 2016
    {
        $match: {
            billing: {
                $gte: ISODate("2016-02-01T00:00:00Z"),
                $lt: ISODate("2016-03-01T00:00:00Z"),
            },
        },
    },

    // Agrupar y sumar ingresos totales en ese mes
    {
        $group: {
            _id: null,
            totalIngresos: { $sum: "$TOTAL" },
        },
    },

    // Proyectar resultado sin _id
    {
        $project: {
            _id: 0,
            totalIngresos: 1,
        },
    },
]);

// (Q6) CONSULTA 6 (: total mensual de visionados (series vs películas))

db.facturas.aggregate([
    // --- Películas ---
    { $unwind: "$Movies" },
    {
        $addFields: {
            mes: { $month: "$Movies.date_time" },
            ano: { $year: "$Movies.date_time" },
        },
    },
    {
        $lookup: {
            from: "Peliculas",
            localField: "Movies.mov_id",
            foreignField: "_id",
            as: "pelidata",
        },
    },
    { $unwind: "$pelidata" },
    {
        $addFields: {
            minutos: {
                $divide: [
                    {
                        $multiply: [
                            "$Movies.viewing_pct",
                            "$pelidata.Details.duration",
                        ],
                    },
                    100,
                ],
            },
        },
    },
    {
        $group: {
            _id: { tipo: "Pelicula", mes: "$mes", ano: "$ano" },
            visionados: { $sum: 1 },
            horas: { $sum: { $divide: ["$minutos", 60] } },
        },
    },
    // --- Series agregadas ---
    {
        $unionWith: {
            coll: "facturas",
            pipeline: [
                { $unwind: "$Series" },
                {
                    $addFields: {
                        mes: { $month: "$Series.date_time" },
                        ano: { $year: "$Series.date_time" },
                    },
                },
                {
                    $lookup: {
                        from: "Series",
                        localField: "Series.serie_id",
                        foreignField: "_id",
                        as: "seriedata",
                    },
                },
                { $unwind: "$seriedata" },
                {
                    $addFields: {
                        minutos: {
                            $divide: [
                                {
                                    $multiply: [
                                        "$Series.viewing_pct",
                                        "$seriedata.avg_duration",
                                    ],
                                },
                                100,
                            ],
                        },
                    },
                },
                {
                    $group: {
                        _id: { tipo: "Serie", mes: "$mes", ano: "$ano" },
                        visionados: { $sum: 1 },
                        horas: { $sum: { $divide: ["$minutos", 60] } },
                    },
                },
            ],
        },
    },
    {
        $project: {
            tipo: "$_id.tipo",
            mes: "$_id.mes",
            ano: "$_id.ano",
            visionados: 1,
            horas: 1,
            _id: 0,
        },
    },
    { $sort: { ano: 1, mes: 1, tipo: 1 } },
]);

// (Q7) CONSULTA 7 (Número total de visionados y duración acumulada.)

db.facturas.aggregate([
    { $unwind: "$Movies" },
    { $match: { "Movies.mov_id": "TT0056172" } },
    {
        $lookup: {
            from: "Peliculas",
            localField: "Movies.mov_id",
            foreignField: "_id",
            as: "pelidata",
        },
    },
    { $unwind: "$pelidata" },
    {
        $addFields: {
            minutos: {
                $divide: [
                    {
                        $multiply: [
                            "$Movies.viewing_pct",
                            "$pelidata.Details.duration",
                        ],
                    },
                    100,
                ],
            },
        },
    },
    {
        $group: {
            _id: "$Movies.mov_id",
            total_visionados: { $sum: 1 },
            minutos_acumulados: { $sum: "$minutos" },
            titulo: { $first: "$pelidata.title" },
        },
    },
    {
        $project: {
            _id: 0,
            pelicula_id: "$_id",
            titulo: 1,
            total_visionados: 1,
            minutos_acumulados: 1,
        },
    },
]);

// (Q8) CONSULTA 8 (Promedio de porcentaje visto por usuario en cada serie.)

db.facturas.aggregate([
    { $unwind: "$Series" },

    {
        $group: {
            _id: { usuario: "$Client.dni", serie: "$Series.serie_id" },
            media_pct: { $avg: "$Series.viewing_pct" },
            total_vistos: { $sum: 1 },
        },
    },

    {
        $project: {
            _id: 0,
            usuario: "$_id.usuario",
            serie: "$_id.serie",
            media_pct: 1,
            total_vistos: 1,
        },
    },
]);
