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
    queryKey: ["estimate", form],
    queryFn: () => listingsService.estimate(form),
    enabled: submitted,
  });

  const set = (field: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    setSubmitted(false);
    setForm((s) => ({ ...s, [field]: field === "km" ? Number(e.target.value || 0) : e.target.value }));
  };

  const result = query.data;

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
        <Card className="space-y-3 p-4">
          <p className="text-sm text-muted-foreground">
            Estimated market price · {result.segment} segment · model v{result.model_version}
          </p>
          <p className="text-3xl font-bold">
            {result.estimated_price_qar.toLocaleString()} QAR
          </p>
          <p className="text-sm text-muted-foreground">
            Confidence range: {result.confidence_range[0].toLocaleString()} – {result.confidence_range[1].toLocaleString()} QAR
          </p>
          <div className="flex gap-4 text-xs text-muted-foreground border-t pt-2">
            <span>R² {result.r2}</span>
            <span>MAPE {result.mape}%</span>
          </div>
          <p className="text-xs text-muted-foreground">
            Create a free account to unlock deal ratings, favorites, alerts, and listing packages.
          </p>
        </Card>
      )}
    </div>
  );
}
