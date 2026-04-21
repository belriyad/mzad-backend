"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { listingsService } from "@/services/listings.service";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";

const FUEL_OPTIONS = ["Petrol", "Diesel", "Hybrid", "Electric", "Natural Gas"];
const GEAR_OPTIONS = ["Automatic", "Manual"];

export default function ValuationPage() {
  const [form, setForm] = useState({
    make: "",
    class_name: "",
    trim: "",
    manufacture_year: "",
    km: 0,
    fuel_type: "",
    gear_type: "",
  });
  const [submitted, setSubmitted] = useState(false);

  const query = useQuery({
    queryKey: ["forecast", form],
    queryFn: () => listingsService.forecast(form),
    enabled: submitted,
  });

  const set = (field: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    setSubmitted(false);
    setForm((s) => ({ ...s, [field]: field === "km" ? Number(e.target.value || 0) : e.target.value }));
  };

  const result = query.data;
  const trendDir = result && result.market_trend_annual_pct > 0 ? "↑" : "↓";
  const trendColor = result && result.market_trend_annual_pct > 0 ? "text-green-500" : "text-red-400";

  return (
    <div className="mx-auto max-w-xl space-y-4">
      <Card className="space-y-3 p-4">
        <h1 className="text-2xl font-semibold">Car Valuator</h1>

        <Input placeholder="Make  (e.g. Toyota)" value={form.make} onChange={set("make")} />
        <Input placeholder="Model  (e.g. Land Cruiser)" value={form.class_name} onChange={set("class_name")} />
        <Input placeholder="Trim  (e.g. GXR, VXR) — optional" value={form.trim} onChange={set("trim")} />
        <Input placeholder="Year  (e.g. 2022)" value={form.manufacture_year} onChange={set("manufacture_year")} />
        <Input type="number" placeholder="Mileage (KM)" value={form.km || ""} onChange={set("km")} />

        <div className="flex gap-2">
          <select
            className="flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm"
            value={form.fuel_type}
            onChange={set("fuel_type")}
          >
            <option value="">Fuel type (optional)</option>
            {FUEL_OPTIONS.map((f) => <option key={f} value={f}>{f}</option>)}
          </select>

          <select
            className="flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm"
            value={form.gear_type}
            onChange={set("gear_type")}
          >
            <option value="">Transmission (optional)</option>
            {GEAR_OPTIONS.map((g) => <option key={g} value={g}>{g}</option>)}
          </select>
        </div>

        <Button
          className="w-full"
          disabled={!form.make || !form.class_name || !form.manufacture_year}
          onClick={() => setSubmitted(true)}
        >
          {query.isFetching ? "Estimating…" : "Estimate Value"}
        </Button>
      </Card>

      {query.isError && (
        <Card className="p-4 border-red-500">
          <p className="text-sm text-red-400">Could not get estimate. Check make, model and year.</p>
        </Card>
      )}

      {result && (
        <>
          {/* Current value */}
          <Card className="space-y-3 p-4">
            <div className="flex items-baseline justify-between">
              <p className="text-sm text-muted-foreground">
                Current value · {result.segment} segment · v{result.model_version}
              </p>
              <span className={`text-sm font-medium ${trendColor}`}>
                Market {trendDir} {Math.abs(result.market_trend_annual_pct)}%/yr
              </span>
            </div>
            <p className="text-3xl font-bold">
              {result.current.estimated_price_qar.toLocaleString()} QAR
            </p>
            <p className="text-sm text-muted-foreground">
              Range: {result.current.confidence_range[0].toLocaleString()} – {result.current.confidence_range[1].toLocaleString()} QAR
            </p>
            <div className="flex gap-4 text-xs text-muted-foreground border-t pt-2">
              <span>R² {result.r2}</span>
              <span>MAPE {result.mape}%</span>
              <span>+{result.annual_km_assumption.toLocaleString()} km/yr assumed</span>
            </div>
          </Card>

          {/* Price forecast */}
          <Card className="p-4 space-y-3">
            <p className="text-sm font-medium">Price forecast</p>
            <div className="grid grid-cols-3 gap-3">
              {result.forecast.map((f) => {
                const isPos = f.change_pct >= 0;
                return (
                  <div key={f.horizon} className="rounded-lg border p-3 space-y-1 text-center">
                    <p className="text-xs text-muted-foreground uppercase tracking-wide">{f.horizon}</p>
                    <p className="text-lg font-semibold">
                      {f.estimated_price_qar.toLocaleString()}
                    </p>
                    <p className={`text-xs font-medium ${isPos ? "text-green-500" : "text-red-400"}`}>
                      {isPos ? "+" : ""}{f.change_pct}%
                    </p>
                  </div>
                );
              })}
            </div>
            <p className="text-xs text-muted-foreground">
              Forecast combines depreciation + Qatar market trend. Assumes {result.annual_km_assumption.toLocaleString()} km/year.
            </p>
          </Card>

          <p className="text-xs text-center text-muted-foreground px-2">
            Create a free account to unlock deal ratings, favorites, alerts, and listing packages.
          </p>
        </>
      )}
    </div>
  );
}
