#!/usr/bin/env python3
"""
Step 7: Scales `nlab_db` up to 1,000,000 pages (or any --target) with full relational integrity.

- Inserts synthetic mathematical wiki records into `pages`
- Inserts corresponding `revisions` with realistic wikitext and Zipf-weighted authors
- Inserts parsed links into `wiki_references` (L, C, R, I) with resolved category FKs
- Updates denormalized aggregates in `users`, `categories`, and `webs`

Usage:
  python database/pipeline/07_generate_1m_pages.py --target 1000000 --batch-size 10000
  python database/pipeline/07_generate_1m_pages.py --dry-run --batch-size 5
"""

from __future__ import annotations

import argparse
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import add_db_args, connect_db, db_config_from_args

PREFIXES = [
    "Abstract", "Abelian", "Algebraic", "Affine", "Annihilating", "Asymptotic",
    "Bimodular", "Categorical", "Cartesian", "Coalgebraic", "Cohomological",
    "Compact", "Complete", "Conformal", "Connected", "Continuous", "Cofibrant",
    "Cyclic", "Deformed", "Derived", "Differential", "Discrete", "Dual",
    "Dynamical", "Enriched", "Equivariant", "Exact", "Fibrant", "Filtered",
    "Finite", "Formal", "Free", "Frobenius", "Fundamental", "Gauge",
    "Geometric", "Graded", "Hermitian", "Higher", "Holomorphic", "Homological",
    "Homotopical", "Hyperbolic", "Infinite", "Internal", "Invariant", "Isometric",
    "Isotropic", "Lagrangian", "Lax", "Linear", "Local", "Modular", "Monoidal",
    "Nilpotent", "Noncommutative", "Normal", "Operadic", "Orthogonal", "Parabolic",
    "Perfect", "Poincaré", "Projective", "Proper", "Pseudo", "Quantum", "Quasi",
    "Rational", "Regular", "Relative", "Riemannian", "Semisimple", "Separable",
    "Sheafified", "Simple", "Simplicial", "Smooth", "Solvable", "Spectral",
    "Stable", "Strict", "Super", "Symmetric", "Symplectic", "Tensor", "Topological",
    "Toric", "Twisted", "Uniform", "Unitary", "Universal", "Weak", "Weighted",
]

CORE_CONCEPTS = [
    "Adjunction", "Algebra", "Algorithm", "Amalgamation", "Approximation",
    "Atlas", "Automaton", "Automorphism", "Bimodule", "Bivector", "Boundary",
    "Bundle", "Category", "Cell", "Chain", "Character", "Class", "Cluster",
    "Cocycle", "Codimension", "Coequalizer", "Cohomology", "Colimit", "Complex",
    "Cone", "Conjecture", "Connection", "Contraction", "Covering", "Curvature",
    "Decomposition", "Deformation", "Degree", "Derivation", "Determinant",
    "Diagram", "Differential", "Dimension", "Discriminant", "Domain", "Duality",
    "Embedding", "Endomorphism", "Ensemble", "Envelope", "Equation", "Equivalence",
    "Expansion", "Extension", "Fiber", "Fibration", "Field", "Filter", "Flow",
    "Form", "Formula", "Fraction", "Framework", "Function", "Functor", "Gauging",
    "Geometry", "Germ", "Grading", "Graph", "Group", "Groupoid", "Hierarchy",
    "Homology", "Homomorphism", "Homotopy", "Hull", "Hypergraph", "Ideal",
    "Identity", "Image", "Immersion", "Index", "Induction", "Infinity-Category",
    "Integral", "Intersection", "Invariant", "Isometry", "Isomorphism", "Kernel",
    "Knot", "Lattice", "Layer", "Lemma", "Lie Algebra", "Limit", "Link",
    "Manifold", "Matrix", "Measure", "Metric", "Model", "Module", "Monad",
    "Morphology", "Morphism", "Multilinear", "Network", "Node", "Number",
    "Object", "Operad", "Operator", "Orbit", "Order", "Orientation",
    "Partition", "Patch", "Path", "Pattern", "Period", "Permutation",
    "Perturbation", "Phase", "Point", "Polygon", "Polytope", "Potential",
    "Presheaf", "Principal Bundle", "Product", "Projection", "Quantum Group",
    "Quotient", "Radical", "Rank", "Ratio", "Ray", "Realization", "Reduction",
    "Refinement", "Relation", "Representation", "Resolution", "Resonance",
    "Restriction", "Retract", "Ring", "Root", "Rotation", "Scheme", "Section",
    "Semigroup", "Sequence", "Series", "Set", "Sheaf", "Shift", "Signature",
    "Singularity", "Site", "Skeleton", "Space", "Spectral Sequence", "Spectrum",
    "Sphere", "Spinor", "Splitting", "Stability", "Stack", "State", "Stratification",
    "Structure", "Subalgebra", "Subcategory", "Subgroup", "Submanifold", "Subobject",
    "Subspace", "Surface", "Surgery", "Suspension", "Symmetry", "System",
    "Target", "Tensor", "Theorem", "Theory", "Threshold", "Topos", "Torsion",
    "Torus", "Transformation", "Translation", "Tree", "Triangle", "Triangulation",
    "Unit", "Valuation", "Variety", "Vector", "Vertex", "Volume", "Wave", "Weight",
]

DOMAINS = [
    "Category Theory", "Algebraic Geometry", "Differential Topology",
    "Homotopy Theory", "Quantum Field Theory", "Mathematical Physics",
    "Abstract Algebra", "Symplectic Geometry", "Functional Analysis",
    "Geometric Group Theory", "Complex Analysis", "Commutative Algebra",
    "Noncommutative Geometry", "Representation Theory", "Number Theory",
    "Arithmetic Geometry", "Lie Theory", "Model Theory", "Proof Theory",
    "Constructive Mathematics", "Higher Category Theory", "Topology",
    "Operator Algebras", "Algebraic Topology", "Differential Geometry",
]

WIKITEXT_TEMPLATE = """# {title}

+-- {{: .num_defn}}
###### Definition
In [[{domain}]], a **{title}** is a [[{core_concept}]] equipped with a canonical morphism to [[{target_link}]].
=--

## Overview

A {title} satisfies standard {adjective} conditions in the context of [[{related_link}]]. For further structural foundations, see also the notion of [[{concept_b}]].

{include_block}

## Examples

* Every standard [[{example_link}]] admits a canonical structure of a {title}.
* In low-dimensional cases, this corresponds to classical constructions in [[{domain}]].

## References

* [[{author_name}]], *Lectures on {title}*, Journal of Higher Mathematics, 20{year}.

[[!category {category_name}]]
{redirect_block}
"""


def generate_unique_titles(needed_count: int, existing_titles: set[str]):
    seen = set(existing_titles)
    rnd = random.Random(42)

    patterns = [
        lambda p, c, d: f"{p} {c}",
        lambda p, c, d: f"{p} {c} in {d}",
        lambda p, c, d: f"{c} of {p} Type",
        lambda p, c, d: f"{p} {c} Theory",
        lambda p, c, d: f"Higher {p} {c}",
        lambda p, c, d: f"{p} {c} on {d}",
        lambda p, c, d: f"{p} {c} of Schemes",
        lambda p, c, d: f"{p} {c} for Bundles",
    ]

    counter = 1
    while len(seen) - len(existing_titles) < needed_count:
        pat = rnd.choice(patterns)
        p = rnd.choice(PREFIXES)
        c = rnd.choice(CORE_CONCEPTS)
        d = rnd.choice(DOMAINS)
        title = pat(p, c, d)

        if title in seen or len(title) > 240:
            title = f"{p} {c} ({counter})"
            counter += 1

        if title not in seen:
            seen.add(title)
            yield title


def weighted_choice(items, weights, rnd):
    return rnd.choices(items, weights=weights, k=1)[0]


class DatabasePopulator:
    def __init__(self, db_config: dict, target_pages: int, batch_size: int, dry_run: bool = False):
        self.db_config = db_config
        self.target_pages = target_pages
        self.batch_size = batch_size
        self.dry_run = dry_run

        self.conn = None
        self.rnd = random.Random(1337)

        self.existing_pages = []
        self.existing_page_names: set[str] = set()
        self.categories = []
        self.users = []
        self.user_name_by_id: dict[int, str] = {}
        self.user_ids = []
        self.user_weights = []
        self.current_page_count = 0
        self.web_id = 1

    def connect(self) -> None:
        if self.dry_run:
            return
        self.conn = connect_db(self.db_config, autocommit=False)

    def load_metadata(self) -> None:
        if self.dry_run:
            self.current_page_count = 20733
            self.categories = [(i, f"cat_{i}") for i in range(1, 55)]
            self.users = [(i, f"user_{i}", 10, 1) for i in range(1, 3327)]
            self.user_name_by_id = {u[0]: u[1] for u in self.users}
            self.user_ids = [u[0] for u in self.users]
            self.user_weights = [1.0 / math.sqrt(i) for i in range(1, len(self.users) + 1)]
            self.existing_page_names = {f"Existing_Page_{i}" for i in range(100)}
            self.existing_pages = [(i, f"Existing_Page_{i}") for i in range(100)]
            return

        assert self.conn is not None
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pages WHERE web_id = %s", (self.web_id,))
            self.current_page_count = cur.fetchone()[0]

            cur.execute("SELECT id, name FROM categories ORDER BY id")
            self.categories = cur.fetchall()

            cur.execute("SELECT id, name, total_revisions, pages_created FROM users ORDER BY total_revisions DESC")
            self.users = cur.fetchall()
            self.user_name_by_id = {u[0]: u[1] for u in self.users}
            self.user_ids = [u[0] for u in self.users]
            self.user_weights = [max(1, int(u[2])) for u in self.users]

            cur.execute("SELECT id, name FROM pages WHERE web_id = %s ORDER BY id", (self.web_id,))
            self.existing_pages = cur.fetchall()
            self.existing_page_names = {p[1] for p in self.existing_pages}

        if not self.users or not self.categories:
            raise RuntimeError(
                "Tables `users` and `categories` must be populated first. Run Steps 1-5 before Step 7."
            )

        print("Loaded database metadata:")
        print(f"  Current pages: {self.current_page_count:,}")
        print(f"  Categories:    {len(self.categories):,}")
        print(f"  Users:         {len(self.users):,}")

    def generate_batch(self, batch_titles: list[str], start_page_id: int):
        pages_data = []
        revisions_data = []
        refs_data = []
        batch_user_revs: dict[int, int] = {}
        batch_user_pages: dict[int, int] = {}
        batch_cat_counts: dict[int, int] = {}

        now = datetime.now()
        base_date = datetime(2014, 1, 1)
        time_span_days = max(1, (now - base_date).days)

        available_target_names = [p[1] for p in self.existing_pages[:1000]] + batch_titles[:500]

        for idx, title in enumerate(batch_titles):
            page_id = start_page_id + idx

            days_offset = self.rnd.randint(0, time_span_days)
            created_at = base_date + timedelta(days=days_offset, seconds=self.rnd.randint(0, 86400))

            r_val = self.rnd.random()
            if r_val < 0.75:
                num_revs = 1
            elif r_val < 0.93:
                num_revs = 2
            elif r_val < 0.98:
                num_revs = 3
            else:
                num_revs = self.rnd.randint(4, 6)

            first_author_id = weighted_choice(self.user_ids, self.user_weights, self.rnd)
            batch_user_pages[first_author_id] = batch_user_pages.get(first_author_id, 0) + 1

            cat_id, cat_name = self.rnd.choice(self.categories)
            batch_cat_counts[cat_id] = batch_cat_counts.get(cat_id, 0) + 1

            domain = self.rnd.choice(DOMAINS)
            core = self.rnd.choice(CORE_CONCEPTS)
            adjective = self.rnd.choice(PREFIXES)
            target1 = self.rnd.choice(available_target_names)
            target2 = self.rnd.choice(available_target_names)
            concept_b = self.rnd.choice(CORE_CONCEPTS)
            example_link = self.rnd.choice(available_target_names)

            include_block = ""
            include_target = None
            if self.rnd.random() < 0.08:
                include_target = f"{domain} Contents"
                include_block = f"[[!include {include_target}]]"

            redirect_block = ""
            alias_name = None
            if self.rnd.random() < 0.15:
                alias_name = f"{title} (Concept)"
                redirect_block = f"[[!redirects {alias_name}]]"

            author_name = self.user_name_by_id.get(first_author_id, "Contributor")
            year = self.rnd.randint(14, 25)

            content = WIKITEXT_TEMPLATE.format(
                title=title,
                domain=domain,
                core_concept=core,
                target_link=target1,
                adjective=adjective,
                related_link=target2,
                concept_b=concept_b,
                include_block=include_block,
                example_link=example_link,
                author_name=author_name,
                year=year,
                category_name=cat_name,
                redirect_block=redirect_block,
            )

            revised_at = created_at
            last_revised_at = created_at

            for rev_idx in range(num_revs):
                if rev_idx == 0:
                    rev_author_id = first_author_id
                    rev_date = created_at
                else:
                    rev_author_id = weighted_choice(self.user_ids, self.user_weights, self.rnd)
                    rev_date = revised_at + timedelta(
                        days=self.rnd.randint(1, 180), seconds=self.rnd.randint(0, 86400)
                    )
                    if rev_date > now:
                        rev_date = now
                    last_revised_at = rev_date

                batch_user_revs[rev_author_id] = batch_user_revs.get(rev_author_id, 0) + 1
                revisions_data.append(
                    (
                        created_at,
                        rev_date,
                        rev_date,
                        page_id,
                        content,
                        rev_author_id,
                        f"192.168.{self.rnd.randint(1, 254)}.{self.rnd.randint(1, 254)}",
                        self.web_id,
                    )
                )

            pages_data.append(
                (
                    page_id,
                    created_at,
                    last_revised_at,
                    self.web_id,
                    None,
                    title,
                    None,
                    num_revs,
                    created_at,
                    last_revised_at,
                )
            )

            refs_data.append((created_at, last_revised_at, page_id, cat_name, "C", None, cat_id, None))
            refs_data.append((created_at, last_revised_at, page_id, domain, "L", None, None, None))
            refs_data.append((created_at, last_revised_at, page_id, core, "L", None, None, None))
            refs_data.append((created_at, last_revised_at, page_id, target1, "L", None, None, None))
            refs_data.append((created_at, last_revised_at, page_id, target2, "L", None, None, None))
            if include_target:
                refs_data.append((created_at, last_revised_at, page_id, include_target, "I", None, None, None))
            if alias_name:
                refs_data.append((created_at, last_revised_at, page_id, alias_name, "R", None, None, None))

        return pages_data, revisions_data, refs_data, batch_user_revs, batch_user_pages, batch_cat_counts

    @staticmethod
    def bulk_insert_rows(cur, table: str, columns: list[str], rows: list[tuple], chunk_size: int = 5000) -> None:
        col_names = ", ".join(f"`{c}`" for c in columns)
        placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"

        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            query = f"INSERT INTO `{table}` ({col_names}) VALUES " + ", ".join([placeholders] * len(chunk))
            flat_params = [val for row in chunk for val in row]
            cur.execute(query, flat_params)

    def run(self) -> None:
        self.connect()
        try:
            self.load_metadata()

            needed_pages = self.target_pages - self.current_page_count
            if needed_pages <= 0:
                print(
                    f"Database already has {self.current_page_count:,} pages "
                    f"(target is {self.target_pages:,}). Nothing to insert."
                )
                return

            print("\n=======================================================")
            print(f"Target:              {self.target_pages:,} pages")
            print(f"Pages to generate:   {needed_pages:,}")
            print(f"Batch size:          {self.batch_size:,}")
            print(f"Dry run:             {self.dry_run}")
            print("=======================================================\n")

            if self.dry_run:
                title_gen = generate_unique_titles(2, self.existing_page_names)
                sample_titles = list(title_gen)
                pages, revs, refs, _, _, _ = self.generate_batch(sample_titles, start_page_id=999000)
                print("[Sample Page Row]:", pages[0])
                print("\n[Sample Revision Wikitext snippet]:\n", revs[0][4][:350], "...\n")
                print(f"[Sample References Generated]: {len(refs)} references for 2 pages")
                for r in refs[:5]:
                    print("  Ref:", r)
                print("\nDry run completed successfully.")
                return

            assert self.conn is not None
            title_gen = generate_unique_titles(needed_pages, self.existing_page_names)
            total_user_revs: dict[int, int] = {}
            total_user_pages: dict[int, int] = {}

            with self.conn.cursor() as cur:
                cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                cur.execute("SET UNIQUE_CHECKS = 0")
                cur.execute("SET autocommit = 0")
                cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM pages")
                next_page_id = cur.fetchone()[0]

            total_inserted = 0
            t0 = time.time()

            page_cols = [
                "id", "created_at", "updated_at", "web_id", "locked_by",
                "name", "locked_at", "revision_count", "first_revised_at", "last_revised_at",
            ]
            rev_cols = ["created_at", "updated_at", "revised_at", "page_id", "content", "author_id", "ip", "web_id"]
            ref_cols = [
                "created_at", "updated_at", "page_id", "referenced_name",
                "link_type", "target_page_id", "category_id", "file_id",
            ]

            while total_inserted < needed_pages:
                batch_count = min(self.batch_size, needed_pages - total_inserted)
                batch_titles = [next(title_gen) for _ in range(batch_count)]
                t_batch_start = time.time()

                pages_data, revs_data, refs_data, u_revs, u_pages, _ = self.generate_batch(
                    batch_titles, start_page_id=next_page_id
                )

                with self.conn.cursor() as cur:
                    self.bulk_insert_rows(cur, "pages", page_cols, pages_data, chunk_size=5000)
                    self.bulk_insert_rows(cur, "revisions", rev_cols, revs_data, chunk_size=3000)
                    self.bulk_insert_rows(cur, "wiki_references", ref_cols, refs_data, chunk_size=5000)

                self.conn.commit()

                for uid, cnt in u_revs.items():
                    total_user_revs[uid] = total_user_revs.get(uid, 0) + cnt
                for uid, cnt in u_pages.items():
                    total_user_pages[uid] = total_user_pages.get(uid, 0) + cnt

                total_inserted += batch_count
                next_page_id += batch_count

                elapsed = time.time() - t0
                speed = total_inserted / max(1.0, elapsed)
                percent = (total_inserted / needed_pages) * 100
                batch_time = time.time() - t_batch_start

                print(
                    f"[{total_inserted:,} / {needed_pages:,} pages ({percent:.1f}%)] "
                    f"Batch: {batch_time:.2f}s | Avg: {speed:.1f} pages/s | Elapsed: {elapsed:.1f}s"
                )

            print("\nAll page batches inserted! Updating aggregate tables...")
            with self.conn.cursor() as cur:
                print("  Updating webs.count_pages...")
                cur.execute(
                    "UPDATE webs SET count_pages = (SELECT COUNT(*) FROM pages WHERE web_id = webs.id), "
                    "updated_at = NOW() WHERE id = %s",
                    (self.web_id,),
                )

                print("  Updating categories.page_count...")
                cur.execute(
                    """
                    UPDATE categories c
                    JOIN (
                        SELECT category_id, COUNT(DISTINCT page_id) AS cnt
                        FROM wiki_references
                        WHERE link_type = 'C' AND category_id IS NOT NULL
                        GROUP BY category_id
                    ) r ON r.category_id = c.id
                    SET c.page_count = r.cnt
                    """
                )

                print("  Updating users.total_revisions and users.pages_created...")
                user_update_rows = [
                    (total_user_revs.get(uid, 0), total_user_pages.get(uid, 0), uid)
                    for uid in total_user_revs
                ]
                for i in range(0, len(user_update_rows), 1000):
                    cur.executemany(
                        """
                        UPDATE users
                        SET total_revisions = total_revisions + %s,
                            pages_created = pages_created + %s,
                            last_edit_at = NOW()
                        WHERE id = %s
                        """,
                        user_update_rows[i : i + 1000],
                    )

                print("  Re-enabling foreign key and unique checks...")
                cur.execute("SET FOREIGN_KEY_CHECKS = 1")
                cur.execute("SET UNIQUE_CHECKS = 1")

            self.conn.commit()
            total_time = time.time() - t0
            print(
                f"\nStep 7 complete: {needed_pages:,} pages added in {total_time:.1f}s "
                f"({total_time / 60:.2f} min, {needed_pages / max(1.0, total_time):.1f} pages/s)."
            )
        finally:
            if self.conn is not None:
                self.conn.close()


def run_generate_pages(
    db_config: dict,
    target_pages: int = 1000000,
    batch_size: int = 10000,
    dry_run: bool = False,
) -> None:
    populator = DatabasePopulator(
        db_config=db_config,
        target_pages=target_pages,
        batch_size=batch_size,
        dry_run=dry_run,
    )
    populator.run()


def main() -> None:
    default_target = int(os.getenv("TARGET_PAGES", "1000000"))
    parser = argparse.ArgumentParser(description="Step 7: Scale nlab_db to 1M pages")
    add_db_args(parser)
    parser.add_argument("--target", type=int, default=default_target, help=f"Target total pages (default: {default_target})")
    parser.add_argument("--batch-size", type=int, default=10000, help="Pages per commit batch (default: 10000)")
    parser.add_argument("--dry-run", action="store_true", help="Generate sample rows without writing to MariaDB")
    args = parser.parse_args()

    run_generate_pages(
        db_config=db_config_from_args(args),
        target_pages=args.target,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
