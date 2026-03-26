import { useState, useEffect, useMemo } from 'react';
import VehicleCard from './VehicleCard';
import axios from 'axios';

export default function Dashboard() {
    const [vehicles, setVehicles] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const [search, setSearch] = useState('');
    const [activeFilter, setActiveFilter] = useState('all');

    useEffect(() => {
        axios.get('http://localhost:8000/api/vehicles')
            .then(res => {
                setVehicles(res.data);
                setLoading(false);
            })
            .catch(err => {
                console.error("Error fetching vehicles:", err);
                setError('Failed to load vehicle data. Is the backend running?');
                setLoading(false);
            });
    }, []);

    const filteredVehicles = useMemo(() => {
        let result = vehicles;
        
        // Search
        if (search) {
            const term = search.toLowerCase();
            result = result.filter(v => 
                v.title.toLowerCase().includes(term) ||
                (v.year && v.year.toString().includes(term)) ||
                (v.price_tnd && v.price_tnd.toString().includes(term))
            );
        }

        // Filter
        if (activeFilter === '2023') {
            result = result.filter(v => v.year === 2023);
        } else if (activeFilter === '2019') {
            result = result.filter(v => v.year && v.year >= 2019);
        } else if (activeFilter === 'luxury') {
            result = result.filter(v => 
                ["audi", "mercedes", "jeep", "bmw"].some(brand => 
                    v.title.toLowerCase().includes(brand)
                )
            );
        } else if (activeFilter === 'good_deals') {
            result = result.filter(v => v.is_good_deal === true);
        }

        return result;
    }, [search, activeFilter, vehicles]);

    return (
        <>
            <div className="background-blobs">
                <div className="blob blob-1"></div>
                <div className="blob blob-2"></div>
                <div className="blob blob-3"></div>
            </div>

            <header>
                <nav>
                    <div className="logo">Bel<span>Khtef</span></div>
                    <div className="search-bar">
                        <input 
                            type="text" 
                            placeholder="Search for your dream car..." 
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                        />
                        <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <circle cx="11" cy="11" r="8"></circle>
                            <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
                        </svg>
                    </div>
                    <div className="nav-actions">
                        <button className="btn-primary">Post Ad</button>
                    </div>
                </nav>
            </header>

            <main>
                <section className="hero">
                    <h1>Find Your Next <span>Perfect Ride</span></h1>
                    <p>Explore the best vehicle deals from Tayara.tn with our premium browser interface.</p>
                </section>

                <section className="filters">
                    {[
                        { id: 'all', label: 'All Vehicles' },
                        { id: '2023', label: 'Latest (2023)' },
                        { id: '2019', label: 'Modern (2019+)' },
                        { id: 'luxury', label: 'Luxury' },
                        { id: 'good_deals', label: '🔥 Good Deals' }
                    ].map(f => (
                        <button 
                            key={f.id}
                            className={`filter-btn ${activeFilter === f.id ? 'active' : ''}`}
                            onClick={() => setActiveFilter(f.id)}
                        >
                            {f.label}
                        </button>
                    ))}
                </section>

                <section className="vehicle-grid">
                    {loading ? (
                        <div className="loader"></div>
                    ) : error ? (
                        <p className="error" style={{gridColumn: "1 / -1", color: "var(--primary)", textAlign: "center"}}>{error}</p>
                    ) : filteredVehicles.length === 0 ? (
                        <p className="no-results" style={{gridColumn: "1 / -1", textAlign: "center"}}>No vehicles found matching your criteria.</p>
                    ) : (
                        filteredVehicles.map((vehicle, idx) => (
                            <VehicleCard key={vehicle.link || idx} vehicle={vehicle} />
                        ))
                    )}
                </section>
            </main>
            
            <footer>
                <p>&copy; 2026 BelKhtef. Data scraped from Tayara.tn</p>
            </footer>
        </>
    );
}
