package store

import (
	"bytes"
	"context"
	"testing"

	cborutil "github.com/filecoin-project/go-cbor-util"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
)

type pair struct {
	K string
	V []byte
}

var tdata []pair = []pair{
	{"ZINC Database", []byte("3D models for molecular docking screens.")},
	{"Yale-CMU-Berkeley (YCB) Object and Model Set", []byte("This project primarily aims to facilitate performance benchmarking in robotics research. The dataset provides mesh models, RGB, RGB-D and point cloud images of over 80 objects. The physical objects are also available via the [YCB benchmarking project](http://www.ycbbenchmarks.com/). The data are collected by two state of the art systems: UC Berkley's scanning rig and the Google scanner. The UC Berkley's scanning rig data provide meshes generated with Poisson reconstruction, meshes generated with volumetric range image integration, textured versions of both meshes, Kinbody files for using the meshes with OpenRAVE, 600 High-resolution RGB images, 600 RGB-D images, and 600 point cloud images for each object. The Google scanner data provides 3 meshes with different resolutions (16k, 64k, and 512k polygons), textured versions of each mesh, Kinbody files for using the meshes with OpenRAVE.")},
	{"Xiph.Org Test Media", []byte("Uncompressed video used for video compression and video processing research.")},
	{"High Resolution Downscaled Climate Data for Southeast Alaska", []byte("This dataset contains historical and projected dynamically downscaled climate data for the Southeast region of the State of Alaska at 1 and 4km spatial resolution and hourly temporal resolution. Select variables are also summarized into daily resolutions. This data was produced using the Weather Research and Forecasting (WRF) model (Version 4.0). We downscaled both Climate Forecast System Reanalysis (CFSR) historical reanalysis data (1980-2019) and both historical and projected runs from two GCM’s from the Coupled Model Inter-comparison Project 5 (CMIP5): GFDL-CM3 and NCAR-CCSM4 (historical run: 1980-2010 and RCP 8.5: 2030-2060).")},
	{"CMIP6 GCMs downscaled using WRF", []byte("High-resolution historical and future climate simulations from 1980-2100")},
	{"Downscaled Climate Data for Alaska", []byte("This dataset contains historical and projected dynamically downscaled climate data for the State of Alaska and surrounding regions at 20km spatial resolution and hourly temporal resolution. Select variables are also summarized into daily resolutions. This data was produced using the Weather Research and Forecasting (WRF) model (Version 3.5). We downscaled both ERA-Interim historical reanalysis data (1979-2015) and both historical and projected runs from 2 GCM’s from the Coupled Model Inter-comparison Project 5 (CMIP5): GFDL-CM3 and NCAR-CCSM4 (historical run: 1970-2005 and RCP 8.5: 2006-2100).")},
	{"DOE's Water Power Technology Office's (WPTO) US Wave dataset", []byte("Released to the public as part of the Department of Energy's Open Energy Data Initiative,\nthis is the highest resolution publicly available long-term wave hindcast\ndataset that – when complete – will cover the entire U.S. Exclusive Economic\nZone (EEZ).\n")},
	{"World Bank - Light Every Night", []byte("Light Every Night - World Bank Nightime Light Data – provides open access to all nightly imagery and data from the Visible Infrared Imaging Radiometer Suite Day-Night Band (VIIRS DNB) from 2012-2020 and the Defense Meteorological Satellite Program Operational Linescan System (DMSP-OLS) from 1992-2013. The underlying data are sourced from the NOAA National Centers for Environmental Information (NCEI) archive. Additional processing by the University of Michigan enables access in Cloud Optimized GeoTIFF format (COG) and search using the Spatial Temporal Asset Catalog (STAC) standard. The data is published and openly available under the terms of the World Bank’s open data license.")},
	{"USGS 3DEP LiDAR Point Clouds", []byte("The goal of the [USGS 3D Elevation Program ](https://www.usgs.gov/core-science-systems/ngp/3dep) (3DEP) is to collect elevation data in the form of light detection and ranging (LiDAR) data over the conterminous United States, Hawaii, and the U.S. territories, with data acquired over an 8-year period. This dataset provides two realizations of the 3DEP point cloud data. The first resource is a public access organization provided in [Entwine Point Tiles](https://entwine.io/entwine-point-tile.html) format, which a lossless, full-density, streamable octree based on [LASzip](https://laszip.org) (LAZ) encoding. The second resource is a [Requester Pays](https://docs.aws.amazon.com/AmazonS3/latest/dev/RequesterPaysBuckets.html) of the same data in LAZ (Compressed LAS) format. Resource names in both buckets correspond to the USGS project names.")},
	{"USGS Landsat", []byte("This joint NASA/USGS program provides the longest continuous space-based record of \nEarth’s land in existence. Every day, Landsat satellites provide essential information \nto help land managers and policy makers make wise decisions about our resources and our environment.\nData is provided for Landsats 1, 2, 3, 4, 5, 7, and 8.\n")},
	{"Covid Job Impacts - US Hiring Data Since March 1 2020", []byte("This dataset provides daily updates on the volume of US job listings filtered by geography industry job family and role; normalized to pre-covid levels.\n\nThese data files feed the business intelligence visuals at covidjobimpacts.greenwich.hr, a public-facing site hosted by Greenwich.HR and OneModel Inc.\nData is derived from online job listings tracked continuously, calculated daily and published nightly.  On average data from 70% of all new US jobs are captured,\nand the dataset currently contains data from 3.3 million hiring organizations.\n\nData for each filter segment is represented as the 7-day average of new job listings for a specific date, expressed as a percentage of the corresponding value \non March 1, 2020.\n")},
	{"UniProt", []byte("The Universal Protein Resource (UniProt) is a comprehensive resource for protein sequence and annotation data. The UniProt databases are the UniProt Knowledgebase (UniProtKB), the UniProt Reference Clusters (UniRef), and the UniProt Archive (UniParc). The UniProt consortium and host institutions [EMBL-EBI](https://www.ebi.ac.uk), [SIB Swiss Institute of Bioinformatics](https://www.sib.swiss) and [PIR](https://proteininformationresource.org/) are committed to the long-term preservation of the [UniProt](https://www.uniprot.org) databases.")},
	{"UK Met Office Atmospheric Deterministic and Probabilistic Forecasts", []byte("Meteorological data reusers now have an exciting opportunity to sample, experiment and evaluate\nMet Office atmospheric model data, whilst also experiencing a transformative method of requesting\ndata via Restful APIs on AWS.\n\nFor information about the data see the [Met Office website](https://www.metoffice.gov.uk/services/data/met-office-data-for-reuse/discovery).\nFor examples of using the data check out the [examples repository](https://github.com/MetOffice/aws-earth-examples).\nIf you need help and support using the data please raise an issue on the examples repository.\n\n **Please note:** Met Office continuously improves and updates its operational forecast models.\nOur last update became effective 04/12/2019. Please find the details [here](https://www.metoffice.gov.uk/services/data/met-office-data-for-reuse/ps43_aws).\n")},
	{"University of British Columbia Sunflower Genome Dataset", []byte("This dataset captures Sunflower's genetic diversity originating\nfrom thousands of wild, cultivated, and landrace sunflower\nindividuals distributed across North America.\n\nThe data consists of raw sequences and associated botanical metadata,\naligned sequences (to three different reference genomes), and sets of\nSNPs computed across several cohorts.\n")},
	{"Transiting Exoplanet Survey Satellite (TESS)", []byte("The Transiting Exoplanet Survey Satellite (TESS) is a multi-year survey that will discover exoplanets in orbit around bright stars across the entire sky using high-precision photometry.  The survey will also enable a wide variety of stellar astrophysics, solar system science, and extragalactic variability studies. More information about TESS is available at [MAST](https://archive.stsci.edu/tess/) and the [TESS Science Support Center](https://heasarc.gsfc.nasa.gov/docs/tess/).\n")},
	{"Terrain Tiles", []byte("A global dataset providing bare-earth terrain heights, tiled for easy usage and provided on S3.")},
	{"Terra Fusion Data Sampler", []byte("The Terra Basic Fusion dataset is a fused dataset of the original Level 1 radiances\nfrom the five Terra instruments. They have been fully validate to contain the original\nTerra instrument Level 1 data. Each Level 1 Terra Basic Fusion file contains one full\nTerra orbit of data and is typically 15 – 40 GB in size, depending on how much data was\ncollected for that orbit. It contains instrument radiance in physical units; radiance\nquality indicator; geolocation for each IFOV at its native resolution; sun-view geometry;\nbservation time; and other attributes/metadata. It is stored in HDF5, conformed to CF\nconventions, and accessible by netCDF-4 enhanced models. It’s naming convention\nfollows: TERRA_BF_L1B_OXXXX_YYYYMMDDHHMMSS_F000_V000.h5. A concise description of the\ndataset, along with links to complete documentation and available software tools, can\nbe found on the Terra Fusion project page: https://terrafusion.web.illinois.edu.</br></br>\n\nTerra is the flagship satellite of NASA’s Earth Observing System (EOS). It was launched\ninto orbit on December 18, 1999 and carries five instruments. These are the\nModerate-resolution Imaging Spectroradiometer (MODIS), the Multi-angle Imaging\nSpectroRadiometer (MISR), the Advanced Spaceborne Thermal Emission and Reflection\nRadiometer (ASTER), the Clouds and Earth’s Radiant Energy System (CERES), and the\nMeasurements of Pollution in the Troposphere (MOPITT).</br></br>\n\nThe Terra Basic Fusion dataset is an easy-to-access record of the Level 1 radiances\nfor instruments on the Terra mission for selected WRS-2 paths covering the years\n2000-2015. These paths are Paths 20-26 (e.g., US corn belt), 108 (e.g., Japan),\n125 (e.g., China), 143 (e.g., India), 150 (e.g., Showa Station, Antarctica),\n169 (e.g., Europe and Africa), 188 (e.g., Nigeria calibration site), and 233\n(e.g., Greenland).\n")},
	{"The Cancer Genome Atlas", []byte("The Cancer Genome Atlas (TCGA), a collaboration between the National Cancer Institute (NCI) and National Human Genome Research Institute (NHGRI), aims to generate comprehensive, multi-dimensional maps of the key genomic changes in major types and subtypes of cancer. TCGA has analyzed matched tumor and normal tissues from 11,000 patients, allowing for the comprehensive characterization of 33 cancer types and subtypes, including 10 rare cancers.\nThe dataset contains open Clinical Supplement, Biospecimen Supplement, RNA-Seq Gene Expression Quantification, miRNA-Seq Isoform Expression Quantification, miRNA Expression Quantification, Genotyping Array Copy Number Segment, Genotyping Array Masked Copy Number Segment, Genotyping Array Gene Level Copy Number Scores, and WXS Masked Somatic Mutation data from Genomic Data Commons (GDC).\nThis dataset also contains controlled Whole Exome Sequencing (WXS), RNA-Seq, miRNA-Seq, ATAC-Seq Aligned Reads, WXS Annotated Somatic Mutation, WXS Raw Somatic Mutation, and WXS Aggregated Somatic Mutation data from GDC.\nTCGA is made available on AWS via the [NIH STRIDES Initiative](https://aws.amazon.com/blogs/publicsector/aws-and-national-institutes-of-health-collaborate-to-accelerate-discoveries-with-strides-initiative/).\n")},
	{"Therapeutically Applicable Research to Generate Effective Treatments (TARGET)", []byte("Therapeutically Applicable Research to Generate Effective Treatments (TARGET) is the collaborative effort of a large, diverse consortium of extramural and NCI investigators. The goal of the effort is to accelerate molecular discoveries that drive the initiation and progression of hard-to-treat childhood cancers and facilitate rapid translation of those findings into the clinic.\nTARGET projects provide comprehensive molecular characterization to determine the genetic changes that drive the initiation and progression of childhood cancers.The dataset contains open Clinical Supplement, Biospecimen Supplement, RNA-Seq Gene Expression Quantification, miRNA-Seq Isoform Expression Quantification, miRNA-Seq miRNA Expression Quantification data from Genomic Data Commons (GDC), and open data from GDC Legacy Archive.\n")},
	{"COVID-19 Harmonized Data", []byte("A harmonized collection of the core data pertaining to COVID-19 reported cases by geography, in a format prepared for analysis")},
	{"Tabula Sapiens", []byte("Tabula Sapiens will be a benchmark, first-draft human cell atlas of two million cells from 25 organs of eight normal human subjects. \nTaking the organs from the same individual controls for genetic background, age, environment, and epigenetic effects, and allows detailed analysis and comparison of cell types that are shared between tissues. \nOur work creates a detailed portrait of cell types as well as their distribution and variation in gene expression across tissues and within the endothelial, epithelial, stromal and immune compartments. \nA critical factor in the Tabula projects is our large collaborative network of PI’s with deep expertise at preparation of diverse organs, enabling all organs from a subject to be successfully processed within a single day. \nTabula Sapiens leverages our network of human tissue experts and a close collaboration with a Donor Network West, a not-for-profit organ procurement organization. \nWe use their experience to balance and assign cell types from each tissue compartment and optimally mix high-quality plate-seq data and high-volume droplet-based data to provide a broad and deep benchmark atlas. \nOur goal is to make sequence data rapidly and broadly available to the scientific community as a community resource. Before you use our data, please take note of our Data Release Policy below.</br></br>\n\nData Release Policy</br></br>\n\nOur goal is to make sequence data rapidly and broadly available to the scientific community as a community resource. It is our intention to publish the work of this project in a timely fashion, and we welcome collaborative interaction on the project and analyses. \nHowever, considerable investment was made in generating these data and we ask that you respect rights of first publication and acknowledgment as outlined in the [Toronto agreement](https://www.nature.com/articles/461168a). \nBy accessing these data, you agree not to publish any articles containing analyses of genes, cell types or transcriptomic data on a whole atlas or tissue scale prior to initial publication by the Tabula Sapiens Consortium and its collaborating scientists. \nIf you wish to make use of restricted data for publication or are interested in collaborating on the analyses of these data, please use email or contact form available from the portal. \nRedistribution of these data should include the full text of the data use policy.\n")},
	{"Tabula Muris", []byte("Tabula Muris is a compendium of single cell transcriptomic data from the model organism *Mus musculus* comprising more than 100,000 cells from 20 organs and tissues. These data represent a new resource for cell biology, reveal gene expression in poorly characterized cell populations, and allow for direct and controlled comparison of gene expression in cell types shared between tissues, such as T-lymphocytes and endothelial cells from different anatomical locations. Two distinct technical approaches were used for most organs: one approach, microfluidic droplet-based 3’-end counting, enabled the survey of thousands of cells at relatively low coverage, while the other, FACS-based full length transcript analysis, enabled characterization of cell types with high sensitivity and coverage. The cumulative data provide the foundation for an atlas of transcriptomic cell biology. See: https://www.nature.com/articles/s41586-018-0590-4\n")},
	{"Tabula Muris Senis", []byte("Tabula Muris Senis is a comprehensive compendium of single cell transcriptomic data from the model organism *Mus musculus* comprising more than 500,000 cells from 18 organs and tissues across the mouse lifespan. We discovered cell-specific changes occurring across multiple cell types and organs, as well as age related changes in the cellular composition of different organs. Using single-cell transcriptomic data we were able to assess cell type specific manifestations of different hallmarks of aging, such as senescence, changes in the activity of metabolic pathways, depletion of stem-cell populations, genomic instability and the role of inflammation as well as other changes in the organism’s immune system. Tabula Muris Senis provides a wealth of new molecular information about how the most significant hallmarks of aging are reflected in a broad range of tissues and cell types.See: https://www.biorxiv.org/content/10.1101/661728v1\n")},
	{"Sudachi Language Resources", []byte("Japanese dictionaries and word embeddings for natural language processing.\n[SudachiDict](https://github.com/WorksApplications/SudachiDict) is the dictionary for a Japanese tokenizer (morphological analyzer) [Sudachi](https://github.com/WorksApplications/Sudachi).\n[chiVe](https://github.com/WorksApplications/chiVe) is Japanese pretrained word embeddings (word vectors), trained using the ultra-large-scale web corpus NWJC by National Institute for Japanese Langauge and Linguistics, analyzed by Sudachi.\n")},
	{"stdpopsim species resources", []byte("Contains all resources (genome specifications, recombination maps, etc.) required for species specific simulation with the stdpopsim package. These resources are originally from a variety of other consortium and published work but are consolidated here for ease of access and use. If you are interested in adding a new species to the stdpopsim resource please raise an issue on the stdpopsim GitHub page to have the necessary files added here.")},
	{"SpaceNet", []byte("SpaceNet, launched in August 2016 as an open innovation project offering a repository of freely available\nimagery with co-registered map features. Before SpaceNet, computer vision researchers had minimal options\nto obtain free, precision-labeled, and high-resolution satellite imagery. Today, SpaceNet hosts datasets\ndeveloped by its own team, along with data sets from projects like IARPA’s Functional Map of the World (fMoW).\n")},
	{"Southern California Earthquake Data", []byte("This dataset contains ground motion velocity and acceleration seismic waveforms recorded by the Southern California Seismic Network (SCSN) and archived at the Southern California Earthquake Data Center (SCEDC).")},
	{"Sophos/ReversingLabs 20 Million malware detection dataset", []byte("A dataset intended to support research on machine learning\ntechniques for detecting malware.  It includes metadata and EMBER-v2\nfeatures for approximately 10 million benign and 10 million malicous\nPortable Executable files, with disarmed but otherwise complete\nfiles for all malware samples.  All samples are labeled using Sophos\nin-house labeling methods, have features extracted using the\nEMBER-v2 feature set, well as metadata obtained via the pefile\npython library, detection counts obtained via ReversingLabs\ntelemetry, and additional behavioral tags that indicate the rough\nbehavior of the samples.\n")},
	{"SondeHub Radiosonde Telemetry", []byte("SondeHub Radiosonde telemetry contains global radiosonde (weather balloon) data captured by SondeHub from our participating radiosonde_auto_rx receiving stations. radiosonde_auto_rx is a open source project aimed at receiving and decoding telemetry from airborne radiosondes using software-defined-radio techniques, enabling study of the telemetry and sometimes recovery of the radiosonde itself.\nCurrently 313 receiver stations are providing data for an average of 384 radiosondes a day.  The data within this repository contains received telemetry frames, including radiosonde type, gps position, and for some radiosondes atmospheric sensor data (temperature, humidity, pressure). As the downlinked telemetry does not always contain calibration information, any atmospheric sensor data should be considered to be uncalibrated. Note that radiosonde_auto_rx does not have sensor data support for all radiosonde types.\n")},
	{"Software Heritage Graph Dataset", []byte("[Software Heritage](https://www.softwareheritage.org/) is the largest\nexisting public archive of software source code and accompanying\ndevelopment history. The Software Heritage Graph Dataset is a fully\ndeduplicated Merkle DAG representation of the Software Heritage archive.\n\nThe dataset links together file content identifiers, source code\ndirectories, Version Control System (VCS) commits tracking evolution over\ntime, up to the full states of VCS repositories as observed by Software\nHeritage during periodic crawls. The dataset’s contents come from major\ndevelopment forges (including GitHub and GitLab), FOSS distributions (e.g.,\nDebian), and language-specific package managers (e.g., PyPI). Crawling\ninformation is also included, providing timestamps about when and where all\narchived source code artifacts have been observed in the wild.\n")},
}

func TestP2P(t *testing.T) {
	log.SetLogLevel("*", "info")
	h1, err := makeBasicHost(3220)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := makeBasicHost(3330)
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h2.SetStreamHandler(PROTOCOL_REQUEST_V1, func(s network.Stream) {
		defer s.Close()
		logging.Info("incoming stream")
		var hmsg RequestMessage
		if err := cborutil.ReadCborRPC(s, &hmsg); err != nil {
			t.Error(err)
			_ = s.Conn().Close()
			return
		}
		logging.Infof("%s", hmsg)

		reply := &ReplyMessage{
			Msg: "ok",
		}
		if err := cborutil.WriteCborRPC(s, reply); err != nil {
			t.Error(err)
		}
	})

	h1.Peerstore().AddAddrs(h2Info.ID, h2Info.Addrs, peerstore.PermanentAddrTTL)

	// err = h1.Connect(ctx, h2Info)
	// if err != nil {
	// 	t.Error(err)
	// }

	s, err := h1.NewStream(ctx, h2Info.ID, PROTOCOL_REQUEST_V1)
	if err != nil {
		t.Error(err)
	}
	defer s.Close()

	req := &RequestMessage{
		Key:    "winner",
		Value:  []byte("leo"),
		Action: ActPut,
	}
	if err := cborutil.WriteCborRPC(s, req); err != nil {
		t.Error(err)
	}

	reply := new(ReplyMessage)
	if err := cborutil.ReadCborRPC(s, reply); err != nil {
		t.Error(err)
		_ = s.Conn().Close()
		return
	}
	logging.Infof("%v", *reply)
}

func TestDataNode(t *testing.T) {
	log.SetLogLevel("*", "info")
	h1, err := makeBasicHost(3220)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := makeBasicHost(3330)
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx := context.Background()
	memStore := ds.NewMapDatastore()

	server := NewStoreServer(ctx, h2, PROTOCOL_V1, memStore)
	defer server.Close()
	server.Serve()

	client := NewStoreClient(ctx, h1, h2Info, PROTOCOL_V1)
	defer client.Close()

	for _, d := range tdata {
		err = client.Put(d.K, d.V)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, d := range tdata {
		has, err := client.Has(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("should have data %s", d.K)
		}
	}

	for _, d := range tdata {
		size, err := client.GetSize(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if size != len(d.V) {
			t.Fatalf("%s size not match", d.K)
		}
	}

	for _, d := range tdata {
		v, err := client.Get(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, d.V) {
			t.Fatalf("%s value not match", d.K)
		}
	}

	for _, d := range tdata {
		err := client.Delete(d.K)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range tdata {
		has, err := client.Has(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if has {
			t.Fatalf("should not have data %s", d.K)
		}
	}

}

func TestDataNodeQuery(t *testing.T) {
	log.SetLogLevel("*", "info")
	h1, err := makeBasicHost(3220)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := makeBasicHost(3330)
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx := context.Background()
	memStore := ds.NewMapDatastore()

	server := NewStoreServer(ctx, h2, PROTOCOL_V1, memStore)
	defer server.Close()
	server.Serve()

	client := NewStoreClient(ctx, h1, h2Info, PROTOCOL_V1)
	defer client.Close()

	for _, d := range tdata {
		err = client.Put(d.K, d.V)
		if err != nil {
			t.Fatal(err)
		}
	}

	results, err := client.Query(dsq.Query{})
	if err != nil {
		t.Fatal(err)
	}
	ents, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != len(tdata) {
		t.Fatalf("count of data not match, expected count: %d, got: %d", len(tdata), len(ents))
	}
	for _, d := range tdata {
		ent, ok := findEntry(ds.NewKey(d.K), ents)
		if !ok {
			t.Fatal("should found data from query result")
		}
		if !bytes.Equal(ent.Value, d.V) {
			t.Fatalf("unexpected value, expected: %s, got: %s", d.V, ent.Value)
		}
	}
}

func findEntry(k ds.Key, ents []dsq.Entry) (dsq.Entry, bool) {
	for _, ent := range ents {
		if ent.Key == k.String() {
			return ent, true
		}
	}
	return dsq.Entry{}, false
}
