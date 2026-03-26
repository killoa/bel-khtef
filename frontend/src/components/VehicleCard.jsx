export default function VehicleCard({ vehicle }) {
    const displayPrice = vehicle.price_tnd
        ? vehicle.price_tnd.toLocaleString() + ' DT'
        : (vehicle.raw_price || 'N/A');

    return (
        <div className="card">
            <div className="image-container">
                <img src={vehicle.image || 'https://via.placeholder.com/400x300?text=No+Image'} alt={vehicle.title} loading="lazy" />
                {vehicle.year && <span className="year-badge">{vehicle.year}</span>}
                {vehicle.is_good_deal && <span className="badge-deal">🔥 Good Deal</span>}
            </div>
            <div className="card-content">
                <h3 className="card-title">{vehicle.title}</h3>
                <div className="card-price">{displayPrice}</div>
                <div className="card-footer">
                    <a href={vehicle.link} target="_blank" rel="noreferrer" className="view-btn">
                        View on Tayara
                        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path><polyline points="15 3 21 3 21 9"></polyline><line x1="10" y1="14" x2="21" y2="3"></line></svg>
                    </a>
                </div>
            </div>
        </div>
    );
}
