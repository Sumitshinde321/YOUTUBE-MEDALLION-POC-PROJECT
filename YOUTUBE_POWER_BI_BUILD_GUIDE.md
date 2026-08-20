# Power BI Dashboard Build Guide: YouTube Trending Video Analysis

This guide provides step-by-step instructions to connect to your PostgreSQL database data warehouse, configure the semantic model, build the three-page report layout, set up page navigations, and establish data alerts in the Power BI Service.

---

## 1. Connection & Refresh Settings

Your project contains a pre-defined Power BI Project file (`YoutubeDashboard.pbip`). When you open this file in Power BI Desktop, it loads the schema, relationships, and DAX measures automatically.

### Database Credentials
If prompted for data source credentials or if you see a connection warning, configure the settings as follows:
* **Data Source Type**: PostgreSQL database
* **Server**: `localhost:5433` (as exposed by the Docker compose port mapping `5433:5432` to the host)
* **Database**: `data_warehouse`
* **Authentication Method**: **Database** (do not use Windows or anonymous)
  * **User name**: `postgres`
  * **Password**: `postgres_secure_pass`
* **Data Connectivity Mode**: **Import**

> [!NOTE]
> The database views `gold.dim_channels`, `gold.dim_videos`, and `gold.fact_video_stats` are automatically loaded into Power BI alongside the table `gold.dim_category`.

---

## 2. Semantic Model Overview

Verify that the relations and measures are active in the **Model View** tab:

### Relationships
* `fact_video_stats[dim_channel_key]` ➔ `dim_channels[dim_channel_key]` (Many-to-One, Single direction)
* `fact_video_stats[dim_video_key]` ➔ `dim_videos[dim_video_key]` (Many-to-One, Single direction)
* `fact_video_stats[dim_category_id]` ➔ `dim_category[category_id]` (Many-to-One, Single direction)
* `fact_video_stats[trending_date]` ➔ `DimDate[Date]` (Many-to-One, Single direction)

### DAX Measures (defined in `fact_video_stats`)
* **Subscriber Count KPI**:
  ```dax
  Subscriber Count KPI = SUM(dim_channels[subscriber_count])
  ```
* **Total Views KPI**:
  ```dax
  Total Views KPI = SUM(fact_video_stats[views])
  ```
* **Total Likes KPI**:
  ```dax
  Total Likes KPI = SUM(fact_video_stats[likes])
  ```
* **Total Dislikes KPI**:
  ```dax
  Total Dislikes KPI = SUM(fact_video_stats[dislikes])
  ```
* **Total Comment Count KPI**:
  ```dax
  Total Comment Count KPI = SUM(fact_video_stats[comment_count])
  ```
* **Subscriber Growth Alert** (binary alert indicator):
  ```dax
  Subscriber Growth Alert = IF([Subscriber Count KPI] > 10000000, 1, 0)
  ```
* **View Spike Alert** (binary alert indicator):
  ```dax
  View Spike Alert = IF([Total Views KPI] > 500000000, 1, 0)
  ```

---

## 3. Visual Layout Configuration

### Page 1: Overview & Trends

This page provides a macro view of the trending videos dataset.

1. **Header**:
   * Insert a **Text Box** visual at the top.
   * Text: `YouTube Trending Video Analysis Dashboard` (Font size: 22, Bold, Centered).
2. **Standalone Date Range Slicer**:
   * Add a **Slicer** visual.
   * Drag `DimDate[Date]` into the field well.
   * Format the slicer as a **Between** range slider.
3. **Line Chart (Views & Likes over Time)**:
   * Add a **Line Chart** visual.
   * **X-Axis**: `DimDate[Date]` (use the Date Hierarchy: `Year`, `Quarter`, `Month`, `Day`).
   * **Y-Axis**: `Total Views KPI` and `Total Likes KPI`.
   * *Drill Operation*: Confirm that the hierarchy is active, allowing users to drill down from Year ➔ Quarter ➔ Month ➔ Day using the header arrows.
4. **Bar Chart (Views by Channel)**:
   * Add a **Clustered Bar Chart** visual.
   * **Y-Axis**: Drag `dim_channels[channel_title]` and then `dim_videos[title]` to create a hierarchy in the same well.
   * **X-Axis**: `Total Views KPI`.
   * *Drill Operation*: Enable drill-down on the visual header to allow navigating from Channel ➔ Video titles.
5. **Pie Chart (Videos by Category)**:
   * Add a **Pie Chart** visual.
   * **Legend**: `dim_category[category_title]`.
   * **Values**: Count of `fact_video_stats[video_id]`.
6. **Navigation Button (P1 ➔ P2)**:
   * Go to **Insert** ➔ **Buttons** ➔ **Blank**.
   * Turn **Action** ON in the Format pane.
   * Set Type to **Page navigation** and Destination to **Video Detail**.
   * Set Button Text to `Go to Video Details ➔`.

---

### Page 2: Video Detail

This page enables granular video research and channel linking.

1. **Synced Slicer (Channel/Category)**:
   * Add a **Slicer** visual.
   * Drag `dim_category[category_title]` and/or `dim_channels[channel_title]` to the fields.
   * Open the **View** menu at the top ribbon and check **Sync Slicers**.
   * In the Sync Slicers pane:
     * Check the **Sync** column checkbox (refresh icon) for all 3 pages (`Overview & Trends`, `Video Detail`, `Q&A and Analytics`).
     * Check the **View** column checkbox (eye icon) for Page 2 (and optionally Page 1/3 if you want the slicer visible there).
2. **Clustered Column Chart (Views and Likes by Channel)**:
   * Add a **Clustered Column Chart** visual.
   * **X-Axis**: Drag `dim_channels[channel_title]` and then `dim_videos[title]` (hierarchy).
   * **Y-Axis**: `Total Views KPI` and `Total Likes KPI`.
   * *Drill Operation*: Turn on drill down to enable diving from Channel ➔ Video titles.
3. **Dynamic Hyperlink Table**:
   * Add a **Table** visual.
   * Drag `dim_channels[channel_title]`, `dim_channels[subscriber_count]`, and `dim_channels[channel_url]` into the columns.
   * Since `channel_url` has the Data Category set to **Web URL** in the semantic model, Power BI automatically renders it as a clickable web icon.
4. **Navigation Button (P2 ➔ P3)**:
   * Insert a **Blank Button**.
   * Action: **Page navigation** ➔ Destination: **Q&A and Analytics**.
   * Button Text: `Go to Q&A & Analytics ➔`.

---

### Page 3: Q&A and Analytics

This page hosts interactive natural language query tools and monthly/weekly trends.

1. **Q&A Visual (Prompt)**:
   * Add a **Q&A** visual from the Visualizations pane.
   * Adjust settings so it suggests sample questions like:
     * "Show total views by channel title as a bar chart"
     * "Which category title has the most likes?"
2. **Combo Chart (Views, Likes & Comments by Date)**:
   * Add a **Line and Stacked Column Chart** or **Line and Clustered Column Chart** visual.
   * **Shared X-Axis**: Drag `DimDate[Month]` and then `DimDate[WeekNo]` to form a hierarchy.
   * **Column Y-Axis**: `Total Views KPI`.
   * **Line Y-Axis**: `Total Likes KPI` and `Total Comment Count KPI`.
   * *Drill Operation*: Enable drill down on the shared axis to navigate from Month ➔ Week.

---

## 4. Publishing & Alert Configurations

Since Data Alerts do not function on local files in Power BI Desktop, you must publish the report and create alerts inside the Power BI Service.

### Publishing
1. Click **Publish** on the Home ribbon of Power BI Desktop.
2. Select your target Power BI workspace (e.g., **My Workspace**) and click Select.
3. Once complete, click the link to open the report in **Power BI Service** (`app.powerbi.com`).

### Setting Up Alerts
1. **Create KPI Cards** (if not already on your pages):
   * Add a Card visual for `Subscriber Count KPI` and a Card visual for `Total Views KPI`.
2. **Pin to Dashboard**:
   * In the Power BI Service report view, hover over the **Subscriber Count KPI** card visual and click the **Pin visual** (thumbtack) icon.
   * Pin it to a new dashboard named `YouTube Analytics Dashboard`.
   * Do the same for the **Total Views KPI** card visual, pinning it to the same dashboard.
3. **Configure Alerts**:
   * Navigate to the **YouTube Analytics Dashboard** in your workspace.
   * Hover over the **Subscriber Count KPI** tile, click the ellipsis (`...`) ➔ select **Manage alerts** (bell icon).
   * Click **+ Add alert rule**:
     * Title: `Subscriber Growth Alert`
     * Condition: **Goes above**
     * Threshold: `10000000` (10M subscribers)
     * Frequency: At most once every 24 hours (or once an hour)
   * Hover over the **Total Views KPI** tile, click the ellipsis (`...`) ➔ select **Manage alerts**:
     * Title: `View Spike Alert`
     * Condition: **Goes above**
     * Threshold: `500000000` (500M total views)
     * Frequency: At most once every 24 hours (or once an hour)
   * Save and close the rules.

---

## 5. Verification Checklist

To verify that your dashboard meets all project requirements:

1. **Verify Slicer Sync**:
   * Filter by a specific Channel/Category on Page 2.
   * Switch to Page 1 and Page 3. Confirm that the data on those pages updates automatically to reflect the same Channel/Category filter.
2. **Verify Drill Operations**:
   * **Line Chart (P1)**: Click the drill-down icon (downward double arrows) and confirm you can navigate from Year ➔ Quarter ➔ Month ➔ Day.
   * **Bar Chart (P1)** / **Column Chart (P2)**: Select the drill-down toggle (single arrow down) and click on a channel bar. It should drill down to show individual videos.
   * **Combo Chart (P3)**: Confirm that drilling down on the X-axis navigates from Month to WeekNo.
3. **Verify Dynamic Link**:
   * In the Page 2 table visual, click on the hyperlink icon in the `channel_url` column. Verify it opens the correct URL in your browser: `https://www.youtube.com/channel/<channel_id>`.
4. **Verify Page Navigation**:
   * Press `Ctrl + Click` on the P1 button to navigate to P2.
   * Press `Ctrl + Click` on the P2 button to navigate to P3.
