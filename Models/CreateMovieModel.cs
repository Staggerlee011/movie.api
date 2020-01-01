namespace MovieApp.Models
{
    public class CreateMovieModel
    {
        public string Name { get; set; }
        public int DirectorId { get; set; }
        public short ReleaseYear { get; set; }
    }
}