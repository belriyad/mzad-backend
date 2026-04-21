import { apiRequest } from "@/services/api-client";
import type { Listing, ListingFilters } from "@/types/listing";

function toQuery(filters: ListingFilters) {
  const q = new URLSearchParams();
  Object.entries(filters).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") {
      q.set(k, String(v));
    }
  });
  return q.toString();
}

type BackendListing = {
  product_id: string | number;
  title?: string;
  price_qar?: number;
  manufacture_year?: string | number;
  km?: number;
  city?: string;
  seller_type?: string | null;
  seller_name?: string | null;
  seller_phone?: string | null;
  warranty_status?: string | null;
  cylinder_count?: number | null;
  listing_date?: string | null;
  main_image_url?: string | null;
  discount_pct?: number | null;
  deal_score?: number | null;
};

function normalizeDealRating(
  discountPct: number | undefined,
  dealScore: number | undefined
): Listing["deal_rating"] {
  const pct = discountPct ?? 0;
  const score = dealScore ?? 0;
  if (pct >= 8 || score >= 2.5) return "great";
  if (pct >= 3 || score >= 1) return "good";
  if (pct >= -3) return "fair";
  return "expensive";
}

function normalizeSellerType(value: string | null | undefined): Listing["seller_type"] {
  if (!value) return "unknown";
  const lower = value.toLowerCase();
  if (lower.includes("dealer")) return "dealer";
  if (lower.includes("owner") || lower.includes("private")) return "owner";
  return "unknown";
}

function normalizeListing(row: BackendListing): Listing {
  const discountPct = row.discount_pct ?? undefined;
  return {
    product_id: String(row.product_id ?? ""),
    title: row.title ?? "Untitled Listing",
    price_qr: Number(row.price_qar ?? 0),
    year: String(row.manufacture_year ?? ""),
    mileage_km: Number(row.km ?? 0),
    city: row.city ?? "Qatar",
    seller_type: normalizeSellerType(row.seller_type),
    seller_name: row.seller_name ?? undefined,
    seller_phone: row.seller_phone ?? undefined,
    warranty_status: row.warranty_status ?? undefined,
    cylinder_count: row.cylinder_count ?? null,
    listing_date: row.listing_date ?? undefined,
    images: row.main_image_url ? [{ url: row.main_image_url }] : [],
    deal_discount_pct: discountPct,
    deal_rating: normalizeDealRating(discountPct, row.deal_score ?? undefined)
  };
}

export const listingsService = {
  list(filters: ListingFilters) {
    const qs = toQuery(filters);
    return apiRequest<{ rows: BackendListing[] }>(`/listings${qs ? `?${qs}` : ""}`).then((res) => ({
      ...res,
      rows: (res.rows ?? []).map(normalizeListing)
    }));
  },
  get(id: string) {
    return apiRequest<BackendListing>(`/listings/${id}`).then(normalizeListing);
  },
  create(payload: Partial<Listing>, token: string) {
    return apiRequest<Listing>(
      "/listings",
      { method: "POST", body: JSON.stringify(payload) },
      token
    );
  },
  update(id: string, payload: Partial<Listing>, token: string) {
    return apiRequest<Listing>(
      `/listings/${id}`,
      { method: "PATCH", body: JSON.stringify(payload) },
      token
    );
  },
  remove(id: string, token: string) {
    return apiRequest<{ ok: boolean }>(`/listings/${id}`, { method: "DELETE" }, token);
  },
  estimate(params: {
    make: string;
    class_name: string;
    trim?: string;
    manufacture_year: string;
    km: number;
    fuel_type?: string;
    gear_type?: string;
  }) {
    const search = new URLSearchParams();
    if (params.make)             search.set("make", params.make);
    if (params.class_name)       search.set("class_name", params.class_name);
    if (params.trim)             search.set("trim", params.trim);
    if (params.manufacture_year) search.set("manufacture_year", params.manufacture_year);
    search.set("km", String(params.km));
    if (params.fuel_type)        search.set("fuel_type", params.fuel_type);
    if (params.gear_type)        search.set("gear_type", params.gear_type);
    return apiRequest<{
      estimated_price_qar: number;
      confidence_range: [number, number];
      segment: string;
      model_version: string;
      r2: number;
      mape: number;
    }>(`/ml/estimate?${search.toString()}`);
  }
};
