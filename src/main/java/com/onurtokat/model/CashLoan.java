package com.onurtokat.model;

public class CashLoan {

    private long oid;
    private int status;
    private long lastupdated;
    private long customeroid;
    private String exchangecode;
    private double loanamount;
    private double exchangerate;
    private String branchcode;
    private String creditreftype;
    private String creditnumber;
    private String creditstatus;
    private long valuedate;
    private long maturitydate;
    private double creditinterest;
    private String productgroupcode;
    private long productrefno;
    private long operationdate;
    private String companycode;
    private int overduetracking;
    private long closingdate;
    private String portfolio;
    private String relatedfilereference;
    private String externalsystemreference;
    private String contracttype;
    private String riskcountry;
    private String transactionoriginator;
    private String limitreference;
    private long proposaloid;
    private int iskds;

    public long getOid() {
        return oid;
    }

    public void setOid(long oid) {
        this.oid = oid;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getLastupdated() {
        return lastupdated;
    }

    public void setLastupdated(long lastupdated) {
        this.lastupdated = lastupdated;
    }

    public long getCustomeroid() {
        return customeroid;
    }

    public void setCustomeroid(long customeroid) {
        this.customeroid = customeroid;
    }

    public String getExchangecode() {
        return exchangecode;
    }

    public void setExchangecode(String exchangecode) {
        this.exchangecode = exchangecode;
    }

    public double getLoanamount() {
        return loanamount;
    }

    public void setLoanamount(double loanamount) {
        this.loanamount = loanamount;
    }

    public double getExchangerate() {
        return exchangerate;
    }

    public void setExchangerate(double exchangerate) {
        this.exchangerate = exchangerate;
    }

    public String getBranchcode() {
        return branchcode;
    }

    public void setBranchcode(String branchcode) {
        this.branchcode = branchcode;
    }

    public String getCreditreftype() {
        return creditreftype;
    }

    public void setCreditreftype(String creditreftype) {
        this.creditreftype = creditreftype;
    }

    public String getCreditnumber() {
        return creditnumber;
    }

    public void setCreditnumber(String creditnumber) {
        this.creditnumber = creditnumber;
    }

    public String getCreditstatus() {
        return creditstatus;
    }

    public void setCreditstatus(String creditstatus) {
        this.creditstatus = creditstatus;
    }

    public long getValuedate() {
        return valuedate;
    }

    public void setValuedate(long valuedate) {
        this.valuedate = valuedate;
    }

    public long getMaturitydate() {
        return maturitydate;
    }

    public void setMaturitydate(long maturitydate) {
        this.maturitydate = maturitydate;
    }

    public double getCreditinterest() {
        return creditinterest;
    }

    public void setCreditinterest(double creditinterest) {
        this.creditinterest = creditinterest;
    }

    public String getProductgroupcode() {
        return productgroupcode;
    }

    public void setProductgroupcode(String productgroupcode) {
        this.productgroupcode = productgroupcode;
    }

    public long getProductrefno() {
        return productrefno;
    }

    public void setProductrefno(long productrefno) {
        this.productrefno = productrefno;
    }

    public long getOperationdate() {
        return operationdate;
    }

    public void setOperationdate(long operationdate) {
        this.operationdate = operationdate;
    }

    public String getCompanycode() {
        return companycode;
    }

    public void setCompanycode(String companycode) {
        this.companycode = companycode;
    }

    public int getOverduetracking() {
        return overduetracking;
    }

    public void setOverduetracking(int overduetracking) {
        this.overduetracking = overduetracking;
    }

    public long getClosingdate() {
        return closingdate;
    }

    public void setClosingdate(long closingdate) {
        this.closingdate = closingdate;
    }

    public String getPortfolio() {
        return portfolio;
    }

    public void setPortfolio(String portfolio) {
        this.portfolio = portfolio;
    }

    public String getRelatedfilereference() {
        return relatedfilereference;
    }

    public void setRelatedfilereference(String relatedfilereference) {
        this.relatedfilereference = relatedfilereference;
    }

    public String getExternalsystemreference() {
        return externalsystemreference;
    }

    public void setExternalsystemreference(String externalsystemreference) {
        this.externalsystemreference = externalsystemreference;
    }

    public String getContracttype() {
        return contracttype;
    }

    public void setContracttype(String contracttype) {
        this.contracttype = contracttype;
    }

    public String getRiskcountry() {
        return riskcountry;
    }

    public void setRiskcountry(String riskcountry) {
        this.riskcountry = riskcountry;
    }

    public String getTransactionoriginator() {
        return transactionoriginator;
    }

    public void setTransactionoriginator(String transactionoriginator) {
        this.transactionoriginator = transactionoriginator;
    }

    public String getLimitreference() {
        return limitreference;
    }

    public void setLimitreference(String limitreference) {
        this.limitreference = limitreference;
    }

    public long getProposaloid() {
        return proposaloid;
    }

    public void setProposaloid(long proposaloid) {
        this.proposaloid = proposaloid;
    }

    public int getIskds() {
        return iskds;
    }

    public void setIskds(int iskds) {
        this.iskds = iskds;
    }

    @Override
    public String toString() {
        return "CashLoan{" +
                "oid=" + oid +
                ", status=" + status +
                ", lastupdated=" + lastupdated +
                ", customeroid=" + customeroid +
                ", exchangecode='" + exchangecode + '\'' +
                ", loanamount=" + loanamount +
                ", exchangerate=" + exchangerate +
                ", branchcode='" + branchcode + '\'' +
                ", creditreftype='" + creditreftype + '\'' +
                ", creditnumber='" + creditnumber + '\'' +
                ", creditstatus='" + creditstatus + '\'' +
                ", valuedate=" + valuedate +
                ", maturitydate=" + maturitydate +
                ", creditinterest=" + creditinterest +
                ", productgroupcode='" + productgroupcode + '\'' +
                ", productrefno=" + productrefno +
                ", operationdate=" + operationdate +
                ", companycode='" + companycode + '\'' +
                ", overduetracking=" + overduetracking +
                ", closingdate=" + closingdate +
                ", portfolio='" + portfolio + '\'' +
                ", relatedfilereference='" + relatedfilereference + '\'' +
                ", externalsystemreference='" + externalsystemreference + '\'' +
                ", contracttype='" + contracttype + '\'' +
                ", riskcountry='" + riskcountry + '\'' +
                ", transactionoriginator='" + transactionoriginator + '\'' +
                ", limitreference='" + limitreference + '\'' +
                ", proposaloid=" + proposaloid +
                ", iskds=" + iskds +
                '}';
    }
}
