tasks = {
    "supplier_library_deprecation_one": """
      Email Thread: VIRTUS AMS: Review Supplier Library for Mirage
      acquisition\nDate of report: 21.08.2025\nReporter: Kering Supply
      Chain\n\nReference environment: PROD Env\n**Bug description and current
      behavior:**\n\nWe need to:\na. Modify the \u201cSupplier Name\u201d of
      \u201cIT01527350126\u201d from \u201cMirage spa\u201d to \u201cMirage DO NOT
      USE\u201d\nb. Add the new supplier \u201cIT04092700121\u201d \u2013
      \u201cMIRAGE SRL\u201d\n\n| 0                                              |
      1                                         | 2                 | 3                     |
      4                            | 5                            | 6                      |
      7                  | 8                | 9                     | 10               |
      \n|:-----------------------------------------------|:------------------------------------------|
      :------------------|:----------------------|:-----------------------------|:-----------------------------|
      :-----------------------|:-------------------|:-----------------|:----------------------|
      :-----------------|\n| TO DO                                          |
      Supplier VAT Number / Registration Number | SAP Supplier Code | Supplier
      Country Code | Supplier Country Description | Supplier Name                |
      Semi Finished Supplier | Supplier Type      | Supplier Status  | Catalogue
      Uploaded By | Visibility Rules |\n| a. review the \"Supplier Name\" of
      IT01527350126 | IT01527350126                             | 100239            |
      IT                    | Italy                        | Mirage spa Mirage DO
      NOT USE | No                     | Frame Manufacturer | Not Active in BC |
      nan                   | No               |\n| b. Add a new supplier                          |
      IT04092700121                             | 107681            | IT                    |
      Italy                        | MIRAGE SRL                   | No                     |
      Frame Manufacturer | Not Active in BC | nan                   | No               |
      \n\n**Expected result:** update the supplier library ad detailed above.
      \n\n**Notes and/or comments:**\nThe supplier \u2018Mirage spa\u2019 has been
      acquired by a third party. As a result, the company name and VAT number have
      been changed respectively to \u2018Mirage SRL\u2019 and
      \u2018IT04092700121\u2019.\n\nWe therefore had to create a new master data
      record in SAP for \u2018Mirage SRL\u2019 and disable the previous code for
      \u2018Mirage spa\u2019.
      """,
    "supplier_library_deprecation_many": """
      **Email Thread:** VIRTUS AMS: Review Supplier Library for LUMINA and ASTRA acquisitions  
    **Date of report:** 04.09.2025  
    **Reporter:** Kering Supply Chain  

    **Reference environment:** PROD Env  

    **Bug description and current behavior:**  

    We need to:  
    a. Modify the “Supplier Name” of “IT02133440987” from “Lumina s.p.a.” to “Lumina DO NOT USE”  
    b. Add the new supplier “IT05876230411” – “LUMINA SRL”  

    c. Modify the “Supplier Name” of “IT03398470122” from “Astra spa” to “Astra DO NOT USE”  
    d. Add the new supplier “IT07845320991” – “ASTRA SRL”  

    | 0   | 1 (Supplier VAT Number / Registration Number) | 2 (SAP Supplier Code) | 3 (Supplier Country Code) | 4 (Supplier Country Description) | 5 (Supplier Name) | 6 (Semi Finished Supplier) | 7 (Supplier Type) | 8 (Supplier Status) | 9 (Catalogue Uploaded By) | 10 (Visibility Rules) |  
    |:---|:----------------------------------------------|:----------------------|:--------------------------|:--------------------------------|:------------------|:----------------------------|:------------------|:--------------------|:--------------------------|:---------------------|  
    | a. review the "Supplier Name" of IT02133440987 | IT02133440987 | 109823 | IT | Italy | Lumina s.p.a. Lumina DO NOT USE | No | Frame Manufacturer | Not Active in BC | nan | No |  
    | b. Add a new supplier | IT05876230411 | 112456 | IT | Italy | LUMINA SRL | No | Frame Manufacturer | Not Active in BC | nan | No |  
    | c. review the "Supplier Name" of IT03398470122 | IT03398470122 | 110542 | IT | Italy | Astra spa Astra DO NOT USE | No | Frame Manufacturer | Not Active in BC | nan | No |  
    | d. Add a new supplier | IT07845320991 | 114877 | IT | Italy | ASTRA SRL | No | Frame Manufacturer | Not Active in BC | nan | No |  

    **Expected result:** update the supplier library as detailed above.  

    **Notes and/or comments:**  
    The suppliers *Lumina s.p.a.* and *Astra spa* have both been acquired by third parties. As a result, their company names and VAT numbers have changed respectively to *LUMINA SRL / IT05876230411* and *ASTRA SRL / IT07845320991*.  

    We therefore had to create new master data records in SAP for *LUMINA SRL* and *ASTRA SRL*, and disable the previous codes for *Lumina s.p.a.* and *Astra spa*.  
    """,
    "685": """
    Number: 685
    Title: Update backend library Supplier
    Body: Can you please add to the \u201cSupplier\u201d Library in all
      PROD/PREPROD env the supplier listed in the table below?\nDue date is the
      end of this week\n\nSupplier VAT Number / Registration Number | SAP Supplier
      Code | Supplier Country | Supplier Name | Semi Finished Supplier | Supplier
      Type Code | Catalogue Uploaded By | Note\n-- | -- | -- | -- | -- | -- | -- | --
      \n9144190007789468XY | \u00a0 | CN | Dongguan Shangpin Glass Products Co.,
      LTD | Yes | Component/Raw Material Supplier | None |
      \u00a0\n913502006120103000 | \u00a0 | CN | Xiamen Torch Special Metal
      Material Co., LTD | No | Component/Raw Material Supplier | None |
      \u00a0\n91440300MA5GLJP574 | \u00a0 | CN | Shenzhen Yushengxin Metal
      Material Co., LTD | No | Component/Raw Material Supplier | None |
      \u00a0\n913303042544926000 | \u00a0 | CN | Wenzhou Hengdeli Metal Materials
      Co., LTD | No | Component/Raw Material Supplier | None |
      \u00a0\n91440300MA5DPCFN0N | \u00a0 | CN | Shenzhen Pinxiang Yulong
      Precision Screw Co., LTD | No | Component/Raw Material Supplier | None |
      \u00a0\n91441900MA53B6PA6K | \u00a0 | CN | Dongguan Changsheng New Material
      Co., LTD | No | Component/Raw Material Supplier | None |
      \u00a0\n91440101MA59R4J03R | \u00a0 | CN | Guangzhou Lerun Composite
      Material Technology Co., LTD | No | Component/Raw Material Supplier | None |
      \u00a0\n91440300MA5FA5CG5W | \u00a0 | CN | Shenzhen Xinhe Xing Glass Co.,
      LTD | No | Component/Raw Material Supplier | None |
      \u00a0\n91441900MA4UHFRN5F | \u00a0 | CN | Dongguan Langfeng Glass Products
      Co., LTD | Yes | Component/Raw Material Supplier | None |
      \u00a0\n91440300359106426U | \u00a0 | CN | Shenzhen Jinaike Metal Material
      Technology Co., Ltd | Yes | Component/Raw Material Supplier | None |
      \u00a0\n91330302MA2AT8PM91 | \u00a0 | CN | Wenzhou Hengli Glasses Co., LTD |
      Yes | Component/Raw Material Supplier | None | \u00a0\n91440300MA5FLEAQ9L |
      \u00a0 | CN | Shenzhen Xingjing Feng Glass Co., LTD | Yes | Component/Raw
      Material Supplier | None | \u00a0\n91331021692399736W | \u00a0 | CN | Yuhuan
      Lula Glasses Co., LTD | Yes | Component/Raw Material Supplier | None |
      \u00a0\n91350200751619373G | \u00a0 | CN | Xiamen Jiyou New Material Co.,
      LTD | Yes | Component/Raw Material Supplier | None |
      \u00a0\n91310115607368434E | \u00a0 | CN | Toray International Trade (China)
      Co., Ltd | Yes | Component/Raw Material Supplier | None |
    """,
    # "delete_all_notifications": """
    # Delete all notifications in test environment
    # """,
    "686": """
Nnumber: 686
Title: VIRTUS AMS - Reset assign Eyewear Arts PROD
Body: Some SKUs were assigned mistakenly to Arts.\nThe following two
SKUs are incorrectly assigned to Arts and need to be addressed.\n\n\nUPC
Code | Material Code | Style Name | Frame Manufacturer VAT Number /
Registration Code | Frame Manufacturer Name | Assigned to Supplier\n-- | -- | -- | -- | -- | --
\n889652313733 | 30009462001 | PU0302O | 09506178-000-02-23-A | Arts Optical
Co. Ltd | ~~Sent~~ Not Sent\n889652287171 | 30008743002 | PU0293O | 09506178-
000-02-23-A | Arts Optical Co. Ltd | ~~Sent~~ Not Sent\n\nPlease refer also
to ticket ID 437 previously managed, the activity is the same.
    """,
    "679": """
GitHub Issue #679
Title: VIRTUS AMS: Review Supplier Library for Mirage acquisition

Email Thread: VIRTUS AMS: Review Supplier Library for Mirage acquisition
Date of report: 21.08.2025
Reporter: Kering Supply Chain

Reference environment: PROD Env
**Bug description and current behavior:**

We need to:
a. Modify the “Supplier Name” of “IT01527350126” from “Mirage spa” to “Mirage DO NOT USE”
b. Add the new supplier “IT04092700121” – “MIRAGE SRL”

| 0                                              | 1                                         | 2                 | 3                     | 4                            | 5                            | 6                      | 7                  | 8                | 9                     | 10               |
|:-----------------------------------------------|:------------------------------------------|:------------------|:----------------------|:-----------------------------|:-----------------------------|:-----------------------|:-------------------|:-----------------|:----------------------|:-----------------|
| TO DO                                          | Supplier VAT Number / Registration Number | SAP Supplier Code | Supplier Country Code | Supplier Country Description | Supplier Name                | Semi Finished Supplier | Supplier Type      | Supplier Status  | Catalogue Uploaded By | Visibility Rules |
| a. review the "Supplier Name" of IT01527350126 | IT01527350126                             | 100239            | IT                    | Italy                        | Mirage spa Mirage DO NOT USE | No                     | Frame Manufacturer | Not Active in BC | nan                   | No               |
| b. Add a new supplier                          | IT04092700121                             | 107681            | IT                    | Italy                        | MIRAGE SRL                   | No                     | Frame Manufacturer | Not Active in BC | nan                   | No               |

**Expected result:** update the supplier library ad detailed above.

**Notes and/or comments:**
The supplier ‘Mirage spa’ has been acquired by a third party. As a result, the company name and VAT number have been changed respectively to ‘Mirage SRL’ and ‘IT04092700121’.

We therefore had to create a new master data record in SAP for ‘Mirage SRL’ and disable the previous code for ‘Mirage spa’.

Comments:
@DJovic90 it has been applied in prod
@fabiobaldassarre11 let's inform Kering...
    """,
    "676": """
GitHub Issue #676
Title: Frame Manufacturer - Galvanic Treatmets catalogue broken export

### Report Mail

_No response_

### Pre-condition

_No response_

### Steps to reproduce

ALL EYEMAN FRONTS
Go to https://omas.cp-bc.com/catalog/galvanicTreatment
Click on the export button

### Expected result

The file should be exported

### Actual result

The catalog is not exported
The Uknown error appears

### What environments are you seeing the problem on?

All env

### Relevant log output

```shell

```

Comments:
@lbobylev  same problem for eyedes and for galvman
@DJovic90 if has been fixed in dev/test/preprod/prod
@lbobylev @iuniavs checked, closing the ticket
""",
    "607": """
GitHub Issue #607
Title: AMS \\ VIRTUS BC 2.0 - Plastics Mapping

**Environment:** PROD

We’d need to change the mapping of the base materials listed in the attached file.

**Expected result:** substitute the “OLD Base Material KEYE Key” (column G in the attached file) with “NEW Base Material KEYE Key” (column K in the attached file).
We expect that all components that have one or more of the base materials mentioned in the attached file will be consequently updated.

[Plastics mapping.xlsx](https://github.com/user-attachments/files/21462335/Plastics.mapping.xlsx)

Comments:
@martinovicm  @ANPI1705  @fabiobaldassarre11
base-materials updated on prod env
@fabiobaldassarre11 did you notify business and did they verified?
"""
}
