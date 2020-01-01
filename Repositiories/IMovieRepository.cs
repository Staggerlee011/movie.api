using MovieApp.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MovieApp.Repositiories
{
    public interface IMovieRepository
    {
        Task<IEnumerable<MovieModel>> GetAllMovies();

        Task<IEnumerable<DirectorModel>> GetAllDirectors();

        Task<MovieModel> GetMovieById(int id);

        Task<int> AddMovie(CreateMovieModel movie);
    }
}