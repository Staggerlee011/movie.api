using Dapper;
using MovieApp.Models;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MovieApp.Repositiories
{
    public class MovieRepository : IMovieRepository
    {
        private readonly ConnectionString _connectionString;

        public MovieRepository(ConnectionString connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<int> AddMovie(CreateMovieModel movie)
        {
            const string query = @"INSERT INTO movies (moviesName, directorsId, releaseYear)
                                   VALUES(@Name, @DirectorId, @ReleaseYear)";

            using (var conn = new NpgsqlConnection(_connectionString.Value))
            {
                var result = await conn.ExecuteAsync(
                    query,
                    new { Name = movie.Name, DirectorId = movie.DirectorId, ReleaseYear = movie.ReleaseYear });
                return result;
            }
        }

        public async Task<IEnumerable<DirectorModel>> GetAllDirectors()
        {
            const string query = @"SELECT d.directorsId ,
                                    d.directorsName ,
                                    m.directorsId ,
                                    m.moviesId ,
                                    m.moviesName MovieName ,
                                    m.releaseYear
                                FROM directors d
                                INNER JOIN movies m ON d.directorsId = m.directorsId";

            using (var conn = new NpgsqlConnection(_connectionString.Value))
            {
                var directorDictionary = new Dictionary<int, DirectorModel>();

                var result = await conn.QueryAsync<DirectorModel, DirectorMovie, DirectorModel>(
                    query,
                    (dir, mov) =>
                    {
                        if (!directorDictionary.TryGetValue(dir.Id, out DirectorModel director))
                        {
                            director = dir;
                            director.Movies = new List<DirectorMovie>();
                            directorDictionary.Add(director.Id, director);
                        }
                        director.Movies.Add(mov);
                        return director;
                    },
                    splitOn: "directorId");

                return result.Distinct();
            }
        }

        public async Task<IEnumerable<MovieModel>> GetAllMovies()
        {
            const string query = @"SELECT m.moviesId,
                                        m.moviesName,
                                        d.directorsName AS DirectorName,
                                        m.ReleaseYear
                                    FROM movies m
                                    INNER JOIN directors d ON m.directorsId = d.directorsId";

            using (var conn = new NpgsqlConnection(_connectionString.Value))
            {
                var result = await conn.QueryAsync<MovieModel>(query);
                return result;
            }
        }

        public async Task<MovieModel> GetMovieById(int id)
        {
            const string query = @"SELECT m.moviesId ,
                                    m.moviesName ,
                                    d.directorsName AS DirectorName ,
                                    m.releaseYear
                                FROM movies m
                                INNER JOIN directors d ON m.directorsId = d.directorsId
                                WHERE m.moviesId = @Id";

            using (var conn = new NpgsqlConnection(_connectionString.Value))
            {
                var result = await conn.QueryFirstOrDefaultAsync<MovieModel>(query, new { Id = id });
                return result;
            }
        }
    }
}